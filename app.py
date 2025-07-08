from flask import Flask, request, jsonify
import requests
import PyPDF2
import io
import re
from urllib.parse import urlparse, parse_qs
import tempfile
import os
import threading
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global health status
health_status = {
    'status': 'healthy',
    'last_check': datetime.now().isoformat(),
    'checks_performed': 0,
    'last_error': None
}

def convert_google_drive_url(url):
    """Convert Google Drive sharing URL to direct download URL"""
    # Extract file ID from various Google Drive URL formats
    patterns = [
        r'/file/d/([a-zA-Z0-9-_]+)',
        r'id=([a-zA-Z0-9-_]+)',
        r'/open\?id=([a-zA-Z0-9-_]+)'
    ]
    
    file_id = None
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            file_id = match.group(1)
            break
    
    if not file_id:
        raise ValueError("Could not extract file ID from Google Drive URL")
    
    # Convert to direct download URL
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def download_pdf(url):
    """Download PDF from URL and return file content"""
    try:
        # Convert Google Drive URL if needed
        if 'drive.google.com' in url:
            url = convert_google_drive_url(url)
        
        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Check if content type is PDF
        content_type = response.headers.get('content-type', '')
        if 'application/pdf' not in content_type and not url.endswith('.pdf'):
            # Sometimes Google Drive doesn't set the correct content type
            pass  # Continue anyway
        
        return response.content
    except Exception as e:
        raise Exception(f"Failed to download PDF: {str(e)}")

def extract_text_from_pdf(pdf_content):
    """Extract text from PDF content"""
    try:
        pdf_file = io.BytesIO(pdf_content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text() + "\n"
        
        return text.strip()
    except Exception as e:
        raise Exception(f"Failed to extract text from PDF: {str(e)}")

def perform_health_check():
    """Perform internal health check"""
    try:
        # Test basic functionality
        test_url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
        
        # Try to download a small test PDF
        response = requests.get(test_url, timeout=10)
        if response.status_code == 200:
            health_status['status'] = 'healthy'
            health_status['last_error'] = None
            logger.info("Health check passed")
        else:
            health_status['status'] = 'warning'
            health_status['last_error'] = f"Test download failed with status {response.status_code}"
            logger.warning(f"Health check warning: {health_status['last_error']}")
            
    except Exception as e:
        health_status['status'] = 'unhealthy'
        health_status['last_error'] = str(e)
        logger.error(f"Health check failed: {e}")
    
    finally:
        health_status['last_check'] = datetime.now().isoformat()
        health_status['checks_performed'] += 1

def health_check_worker():
    """Background worker for periodic health checks"""
    while True:
        perform_health_check()
        time.sleep(300)  # 5 minutes = 300 seconds

def start_health_check_thread():
    """Start the health check background thread"""
    health_thread = threading.Thread(target=health_check_worker, daemon=True)
    health_thread.start()
    logger.info("Health check thread started - will run every 5 minutes")

@app.route('/extract-text', methods=['POST'])
def extract_text():
    """Main endpoint to extract text from PDF via Google Drive link"""
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data or 'url' not in data:
            return jsonify({
                'error': 'Missing required field: url',
                'status': 'error'
            }), 400
        
        pdf_url = data['url']
        
        # Validate URL
        if not pdf_url.startswith('http'):
            return jsonify({
                'error': 'Invalid URL format',
                'status': 'error'
            }), 400
        
        # Download PDF
        pdf_content = download_pdf(pdf_url)
        
        # Extract text
        extracted_text = extract_text_from_pdf(pdf_content)
        
        return jsonify({
            'text': extracted_text,
            'status': 'success',
            'message': 'Text extracted successfully'
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint with internal monitoring status"""
    return jsonify({
        'status': health_status['status'],
        'message': 'PDF text extraction server health status',
        'last_check': health_status['last_check'],
        'checks_performed': health_status['checks_performed'],
        'last_error': health_status['last_error'],
        'timestamp': datetime.now().isoformat()
    })

@app.route('/', methods=['GET'])
def home():
    """Home endpoint with usage instructions"""
    return jsonify({
        'message': 'PDF Text Extraction Server',
        'usage': {
            'endpoint': '/extract-text',
            'method': 'POST',
            'content-type': 'application/json',
            'body': {
                'url': 'https://drive.google.com/file/d/your-file-id/view'
            },
            'response': {
                'text': 'extracted text content',
                'status': 'success',
                'message': 'Text extracted successfully'
            }
        },
        'examples': [
            'POST /extract-text with {"url": "https://drive.google.com/file/d/1abc123/view"}',
            'GET /health for health check'
        ]
    })

if __name__ == '__main__':
    # Start the health check thread
    start_health_check_thread()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)