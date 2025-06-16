import requests
import os
import json
import argparse
from urllib.parse import urlencode
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

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

    response = requests.post(url, data=payload, headers=headers)
    print(f"🔑 Token request status: {response.status_code}")
    data = response.json()
    return data['access_token']


def download_attachments_for_article(table_sys_id, output_dir, headers):
    attachment_url = f"https://lendlease.service-now.com/api/now/attachment?sysparm_query=table_sys_id={table_sys_id}"

    def try_download(headers):
        try:
            response = requests.get(attachment_url, headers=headers)
            if response.status_code == 401:
                return 'unauthorized', None
            elif response.status_code != 200:
                print(f"❌ Failed to get attachments for {table_sys_id}. Status: {response.status_code}")
                return 'failed', None

            data = response.json()
            attachments = data.get('result', [])
            if not attachments:
                print(f"📎 No attachments found for {table_sys_id}")
                return 'empty', None

            print(f"📎 Found {len(attachments)} attachment(s) for {table_sys_id}")
            return 'success', attachments
        except Exception as e:
            print(f"❌ Exception: {e}")
            return 'error', None

    status, attachments = try_download(headers)

    if status == 'unauthorized':
        print("🔄 Refreshing token for attachment download...")
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
                    print(f"   ✓ Downloaded attachment: {file_name} ({file_size} bytes)")
                else:
                    print(f"   ✗ Failed to download {file_name} (Status {file_response.status_code})")
            except Exception as e:
                print(f"   ✗ Error downloading {file_name}: {e}")


def download_servicenow_pdf(sys_id, pdf_dir, headers):
    url = f"https://lendlease.service-now.com/sn_hr_core_case.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view"
    response = requests.get(url, headers=headers)

    if response.status_code == 401:
        print("🔄 Token expired while downloading PDF, refreshing token...")
        bearer_token = get_bearer_token()
        headers['Authorization'] = f'Bearer {bearer_token}'
        response = requests.get(url, headers=headers)

    if response.status_code == 200:
        filename = f"{sys_id}.pdf"
        file_path = os.path.join(pdf_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"   ✓ PDF successfully saved as {file_path}")
    else:
        print(f"   ✗ Failed to download PDF for sys_id {sys_id}. Status: {response.status_code}")
        print(response.text)


def download_all_attachments_and_pdfs(json_file, headers):
    with open(json_file, 'r') as f:
        response_data = json.load(f)

    tickets = response_data.get("result", [])
    print(f"🎫 Processing {len(tickets)} ticket(s)...")

    # Create master folder with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    master_folder = f"HR_Tickets_{timestamp}"
    os.makedirs(master_folder, exist_ok=True)

    for ticket in tickets:
        sys_id = ticket.get("sys_id")
        ticket_number = ticket.get("number", sys_id)

        if not sys_id:
            print("❌ Skipping ticket with missing sys_id")
            continue

        print(f"\n📥 Ticket: {ticket_number} (sys_id: {sys_id})")

        base_dir = os.path.join(master_folder, ticket_number)
        attachment_dir = os.path.join(base_dir, "Attachments")
        pdf_dir = os.path.join(base_dir, "PDFs")

        os.makedirs(attachment_dir, exist_ok=True)
        os.makedirs(pdf_dir, exist_ok=True)

        download_attachments_for_article(sys_id, attachment_dir, headers)
        download_servicenow_pdf(sys_id, pdf_dir, headers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download all attachments and PDFs from ServiceNow tickets.')
    parser.add_argument('json_path', type=str, help='Path to the response.json file')
    args = parser.parse_args()

    token = get_bearer_token()
    headers = {
        'Authorization': f'Bearer {token}',
    }

    download_all_attachments_and_pdfs(args.json_path, headers)
