import requests
import os
import json
import argparse
from urllib.parse import urlencode
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv()

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # <-- Use seconds for extra uniqueness
log_dir = f'logs_{timestamp}'
os.makedirs(log_dir, exist_ok=True)

# General log file
log_file = os.path.join(log_dir, f'general_{timestamp}.log')
# Error log file
error_log_file = os.path.join(log_dir, f'error_{timestamp}.log')

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),  # <-- Add encoding
        logging.StreamHandler()
    ]
)

# Create error logger
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler(error_log_file, encoding='utf-8')  # <-- Add encoding
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

def get_bearer_token():
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
        logging.info(f"🔑 Token request status: {response.status_code}")
        data = response.json()
        return data['access_token']
    except Exception as e:
        error_logger.error(f"Failed to get bearer token: {e}")
        raise

def download_attachments_for_article(table_sys_id, output_dir, headers, ticket_number):
    attachment_url = f"https://lendlease.service-now.com/api/now/attachment?sysparm_query=table_sys_id={table_sys_id}"

    def try_download(headers):
        try:
            response = requests.get(attachment_url, headers=headers)
            if response.status_code == 401:
                return 'unauthorized', None
            elif response.status_code != 200:
                logging.error(f"❌ Failed to get attachments for ticket {ticket_number} (Status: {response.status_code})")
                error_logger.error(f"❌ Failed to get attachments for ticket {ticket_number} (Status: {response.status_code})")
                return 'failed', None

            data = response.json()
            attachments = data.get('result', [])
            if not attachments:
                logging.info(f"📎 No attachments found for ticket {ticket_number}")
                return 'empty', None

            logging.info(f"📎 Found {len(attachments)} attachment(s) for ticket {ticket_number}")
            return 'success', attachments
        except Exception as e:
            error_logger.error(f"❌ Exception getting attachments for ticket {ticket_number}: {e}")
            return 'error', None

    status, attachments = try_download(headers)

    if status == 'unauthorized':
        logging.info("🔄 Refreshing token for attachment download...")
        headers['Authorization'] = f'Bearer {get_bearer_token()}'
        status, attachments = try_download(headers)
        if status != 'success':
            return

    if status != 'success':
        return

    for attachment in attachments:
        file_name = attachment.get('file_name')
        sys_id = attachment.get('sys_id')
        file_name = f"{sys_id}_{file_name}" if file_name else f"{table_sys_id}_attachment"
        download_link = attachment.get('download_link')
        file_size = attachment.get('size_bytes')

        if download_link and file_name:
            try:
                file_response = requests.get(download_link, headers=headers)
                if file_response.status_code == 200:
                    file_path = os.path.join(output_dir, file_name)
                    with open(file_path, 'wb') as f:
                        f.write(file_response.content)
                    logging.info(f"   ✓ Downloaded attachment '{file_name}' for ticket {ticket_number} ({file_size} bytes)")
                else:
                    error_logger.error(f"   ✗ Failed to download attachment '{file_name}' for ticket {ticket_number} (Status {file_response.status_code})")
            except Exception as e:
                error_logger.error(f"   ✗ Error downloading attachment '{file_name}' for ticket {ticket_number}: {e}")

def download_servicenow_pdf(sys_id, pdf_dir, headers, ticket_number):
    url = f"https://lendlease.service-now.com/x_llusn_bankg_bi_req.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view"
    response = requests.get(url, headers=headers)

    if response.status_code == 401:
        logging.info("🔄 Token expired while downloading PDF, refreshing token...")
        bearer_token = get_bearer_token()
        headers['Authorization'] = f'Bearer {bearer_token}'
        response = requests.get(url, headers=headers)

    if response.status_code == 200:
        filename = f"{sys_id}.pdf"
        file_path = os.path.join(pdf_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        logging.info(f"   ✓ PDF successfully saved for ticket {ticket_number} as {file_path}")
    else:
        logging.error(f"   ✗ Failed to download PDF for ticket {ticket_number}, sys_id {sys_id}. Status: {response.status_code}")
        error_logger.error(f"   ✗ Failed to download PDF for ticket {ticket_number}, sys_id {sys_id}. Status: {response.status_code}")
        error_logger.error(f"   ✗ PDF download failure details for ticket {ticket_number}: {response.text}")

def download_all_attachments_and_pdfs(json_file, headers):
    with open(json_file, 'r') as f:
        response_data = json.load(f)

    tickets = response_data.get("result", [])
    logging.info(f"🎫 Processing {len(tickets)} ticket(s)...")

    # Create master folder with timestamp (use the same timestamp as logs)
    master_folder = f"Treasury_Tickets_attachments_and_pdfs_{json_file}_{timestamp}"
    os.makedirs(master_folder, exist_ok=True)

    for ticket in tickets:
        sys_id = ticket.get("sys_id")
        ticket_number = ticket.get("number", sys_id)

        if not sys_id:
            logging.error("❌ Skipping ticket with missing sys_id")
            error_logger.error("❌ Skipping ticket with missing sys_id")
            continue

        logging.info(f"\n📥 Ticket: {ticket_number} (sys_id: {sys_id})")

        base_dir = os.path.join(master_folder, ticket_number)
        attachment_dir = os.path.join(base_dir, "Attachments")
        pdf_dir = os.path.join(base_dir, "PDFs")

        os.makedirs(attachment_dir, exist_ok=True)
        os.makedirs(pdf_dir, exist_ok=True)

        download_attachments_for_article(sys_id, attachment_dir, headers, ticket_number)
        download_servicenow_pdf(sys_id, pdf_dir, headers, ticket_number)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download all attachments and PDFs from ServiceNow tickets.')
    parser.add_argument('json_path', type=str, help='Path to the response.json file')
    args = parser.parse_args()

    token = get_bearer_token()
    headers = {
        'Authorization': f'Bearer {token}',
    }

    download_all_attachments_and_pdfs(args.json_path, headers)
