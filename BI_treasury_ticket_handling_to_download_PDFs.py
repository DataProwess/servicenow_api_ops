import requests
import os
import json
import argparse
from urllib.parse import urlencode
from dotenv import load_dotenv
from datetime import datetime
import logging
import time
import re



load_dotenv()

# Create a unique timestamp for folder and files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define log folder and files with timestamp
log_dir = f"BI_treasury_total_logs_{timestamp}"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"bi_treasury_general_{timestamp}.log")

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
def log_error_to_file(message, log_dir=log_dir, timestamp=timestamp):
    """Log error to file only when errors occur."""
    error_log_file = os.path.join(log_dir, f'bi_treasury_error_{timestamp}.log')
    os.makedirs(log_dir, exist_ok=True)
    with open(error_log_file, 'a', encoding='utf-8') as f:
        timestamp_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{timestamp_now} - ERROR - {message}\n")



# Token cache to avoid repeated requests
token_cache = {'value': None, 'expires': 0}

def get_bearer_token():
    global token_cache
    if time.time() < token_cache['expires']:
        return token_cache['value']

    url = "https://lendlease.service-now.com/oauth_token.do"
    payload_dict = {
        'grant_type': 'password',
        'username': os.getenv('SNOW_USERNAME'),
        'password': os.getenv('SNOW_PASSWORD'),
        'client_id': os.getenv('SNOW_CLIENT_ID'),
        'client_secret': os.getenv('SNOW_CLIENT_SECRET')
    }
    payload = urlencode(payload_dict)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        token_cache = {
            'value': data['access_token'],
            'expires': time.time() + data['expires_in'] - 360  # Subtract 360s for buffer
        }
        return data['access_token']
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Token request failed: {str(e)}")
        log_error_to_file(f"❌ Token request failed: {str(e)}")
        raise

def sanitize_filename(filename):
    # Replace or remove illegal characters
    illegal_chars = r'[<>:"/\\|?*\0]'
    filename = re.sub(illegal_chars, '_', filename)
    # Remove leading/trailing spaces and dots (Windows restrictions)
    filename = filename.strip('. ').strip()
    return filename


                

def download_servicenow_pdf(sys_id, pdf_dir, headers, ticket_number):
    url = f"https://lendlease.service-now.com/x_llusn_bankg_bonding_instrument.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view"
    response = requests.get(url, headers=headers)

    if response.status_code == 401:
        logging.info("Token expired while downloading PDF, refreshing token...")
        bearer_token = get_bearer_token()
        headers['Authorization'] = f'Bearer {bearer_token}'
        response = requests.get(url, headers=headers)

    if response.status_code == 200:
        filename = f"{ticket_number}_{sys_id}.pdf"
        file_path = os.path.join(pdf_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        logging.info(f"PDF successfully saved for ticket={ticket_number} as {file_path}")
    else:
        msg = f"Failed to download PDF for ticket= {ticket_number}, sys_id {sys_id}. Status: {response.status_code}"
        logging.error(msg)
        log_error_to_file(msg)
        log_error_to_file(f"PDF download failure details for ticket= {ticket_number}, table_sys_id= {sys_id}. : {response.text}")

def download_all_attachments_and_pdfs(json_file, headers):
    with open(json_file, 'r') as f:
        response_data = json.load(f)

    tickets = response_data.get("result", [])
    logging.info(f"Processing {len(tickets)} ticket(s)...")

    # Create master folder with timestamp
    trimmed_path = json_file[:-5]
    master_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    master_folder = f"BI_Treasury_Tickets_pdfs_{trimmed_path}_{master_timestamp}"
    os.makedirs(master_folder, exist_ok=True)

    for ticket in tickets:
        sys_id = ticket.get("sys_id")
        ticket_number = ticket.get("number")

        if not sys_id:
            msg = "Skipping ticket with missing sys_id"
            logging.error(msg)
            log_error_to_file(msg)
            continue

        logging.info(f"Ticket: {ticket_number} (sys_id: {sys_id})")

        base_dir = os.path.join(master_folder, ticket_number)
        # attachment_dir = os.path.join(base_dir, "Attachments")
        pdf_dir = os.path.join(base_dir, "PDFs")

        # os.makedirs(attachment_dir, exist_ok=True)
        os.makedirs(pdf_dir, exist_ok=True)

        # download_attachments_for_article(sys_id, attachment_dir, headers, ticket_number)
        download_servicenow_pdf(sys_id, pdf_dir, headers, ticket_number)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download all BI Treasury PDFs from ServiceNow tickets.')
    parser.add_argument('json_path', type=str, help='Path to the response.json file')
    args = parser.parse_args()

    token = get_bearer_token()
    headers = {
        'Authorization': f'Bearer {token}',
    }

    download_all_attachments_and_pdfs(args.json_path, headers)
    
    
   
    print("\033[92mAll BI Treasury PDFs downloaded successfully.\033[0m")
    logging.info("✅ All BI Treasury PDFs downloaded successfully.")

