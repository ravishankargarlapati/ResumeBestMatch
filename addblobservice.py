import io
import os
import PyPDF2
from docx import Document
import xml.etree.ElementTree as ET
import azure.functions as func
import requests

def extract_text_from_pdf(filename):
    """
    Extract text from a PDF file.
    
    Args:
        filename (str): The path to the PDF file.
        
    Returns:
        str: The extracted text from the PDF file.
    """
    try:
        with open(filename, 'rb') as f:
            # Create a PDF file reader object
            pdf_reader = PyPDF2.PdfReader(f)
            
            # Initialize an empty string to store the content
            content = ''
            
            # Iterate through each page and extract text
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                content += page.extract_text()
            
            # Check if content is empty
            if not content.strip():
                print(f"Content is empty in PDF file: {filename}")
                return None
            
        return content
    except Exception as e:
        print(f"An error occurred while reading PDF file {filename}: {e}")
        return None

def extract_text_from_docx(docx_data):
    """
    Extract text from a DOCX file.
    
    Args:
        docx_data (bytes): The binary data of the DOCX file.
        
    Returns:
        str: The extracted text from the DOCX file.
    """
    text = ""
    doc = Document(io.BytesIO(docx_data))
    for paragraph in doc.paragraphs:
        text += paragraph.text
    return text

def calculate_keyword_existence(text, keywords):
    """
    Calculate the existence of keywords in a given text.
    
    Args:
        text (str): The text to search for keywords.
        keywords (list): A list of keywords to search for.
        
    Returns:
        float: The existence of keywords in the text.
    """
    text = text.lower()
    keyword_count = sum(text.count(keyword.lower()) for keyword in keywords)
    total_keywords = len(keywords)
    existence = keyword_count / (keyword_count + total_keywords) if keyword_count > 0 else 0
    return existence

def fetch_blob_urls(testurl):
    """
    Fetch blob URLs from a given URL.
    
    Args:
        testurl (str): The URL to fetch blob URLs from.
        
    Returns:
        list: A list of blob URLs.
    """
    try:
        # Send HTTP GET request to fetch the XML response
        response = requests.get(testurl)
        if response.status_code == 200:
            # Parse the XML response
            root = ET.fromstring(response.content)
            
            # Extract URLs of the files
            urls = [blob.find('Url').text for blob in root.findall('.//Blob')]
            
            return urls
        else:
            print(f"Failed to fetch blob URLs. Status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while fetching blob URLs: {e}")
        return None

def download_files(urls):
    """
    Download files from a list of URLs and extract text from them.
    
    Args:
        urls (list): A list of URLs to download files from.
        
    Returns:
        list: A list of tuples containing filename and extracted text.
    """
    resume_data = []
    if urls is None:
        return
    
    try:
        for url in urls:
            # Send HTTP GET request to download the file
            response = requests.get(url)
            if response.status_code == 200:
                # Extract the filename from the URL
                filename = url.split('/')[-1]
                
                # Save the file locally
                with open(filename, 'wb') as f:  # Open in binary mode
                    f.write(response.content)  # Write content to the file
                    
                if filename.endswith('.pdf'):
                    text = extract_text_from_pdf(filename)  # Extract text from PDF
                    resume_data.append((filename, text))
                elif filename.endswith('.docx'):
                    with open(filename, 'rb') as f:  # Open in binary mode
                        text = extract_text_from_docx(f.read())  # Extract text from DOCX
                        resume_data.append((filename, text))
            else:
                print(f"Failed to download file from URL {url}. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"An error occurred while downloading files: {e}")
    return resume_data

def process_resume_data(keywords, required_count, threshold, bloburl):
    """
    Process resume data based on provided parameters.
    
    Args:
        keywords (list): A list of keywords to search for.
        required_count (int): The number of records to print.
        threshold (float): The threshold for keyword existence.
        bloburl (str): The URL to fetch blob URLs from.
        
    Returns:
        list: A list of tuples containing filename and keyword existence.
    """
    # Load test resume data
    test_resume_data_url = fetch_blob_urls(bloburl)
    test_resume_data = download_files(test_resume_data_url)
    
    # Verify keyword existence for test data
    test_keyword_existence = [(file_name, calculate_keyword_existence(text, keywords)) for file_name, text in test_resume_data]
    
    # Sort the existence in descending order
    test_keyword_existence.sort(key=lambda x: x[1], reverse=True)
    
    # Return processed resume data
    return test_keyword_existence[:required_count]

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        keywords = req_body.get('keywords', '').split(',')
        required_count = int(req_body.get('required_count', 5))
        threshold = float(req_body.get('threshold', 0.5))
        bloburl = req_body.get('bloburl', '')

        result = process_resume_data(keywords, required_count, threshold, bloburl)
        output = "\nResult:\n"
        for file_name, exists in result:
            output += f"File: {file_name}, Keyword accuracy: {exists}\n"

        return func.HttpResponse(output, mimetype="text/plain", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
