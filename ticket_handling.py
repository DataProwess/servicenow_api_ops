import requests
import os
import json
import argparse
from urllib.parse import urlencode
import datetime
from dotenv import load_dotenv

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

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    response = requests.post(url, data=payload, headers=headers)
    print(f"Response Status Code: {response.status_code}")
    data = response.json()
    print(data)
    return data['access_token'] 

# Your existing API call

payload = {}

token = get_bearer_token()
headers = {
    
    'Authorization': f'Bearer {token}',
}

 
def download_attachments_for_article(table_sys_id, output_dir, headers):
    """Download attachments for a specific KB article and save them in its folder,
    refresh token if 401 Unauthorized is received."""

    attachment_url = f"https://lendlease.service-now.com/api/now/attachment?sysparm_query=table_sys_id={table_sys_id}"

    def try_download(headers):
        try:
            response = requests.get(attachment_url, headers=headers)
            if response.status_code == 401:
                return 'unauthorized', None
            elif response.status_code != 200:
                print(f"❌ Failed to get attachment list for {table_sys_id}. Status code: {response.status_code}")
                return 'failed', None

            data = response.json()
            attachments = data.get('result', [])
            if not attachments:
                print(f"📎 No attachments found for {table_sys_id}")
                return 'empty', None

            print(f"📎 Found {len(attachments)} attachment(s) for {table_sys_id}")
            return 'success', attachments
        except Exception as e:
            print(f"❌ Exception while fetching attachments: {e}")
            return 'error', None

    status, attachments = try_download(headers)

    if status == 'unauthorized':
        print("🔄 Access token expired, refreshing token...")
        # Refresh token here and update headers
        new_token = get_bearer_token()
        headers['Authorization'] = f'Bearer {new_token}'
        # Retry once with new token
        status, attachments = try_download(headers)
        if status == 'unauthorized':
            print("❌ Token refresh failed or new token also unauthorized.")
            return
        elif status != 'success':
            return

    if status != 'success':
        return

    # Download each attachment
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
                    print(f"   ✓ Downloaded: {file_name} ({file_size} bytes)")
                else:
                    print(f"   ✗ Failed to download {file_name} (Status {file_response.status_code})")
            except Exception as e:
                print(f"   ✗ Error downloading {file_name}: {e}")

def download_servicenow_pdf(sys_id, pdf_dir,headers):
    url = f"https://lendlease.service-now.com/sn_hr_core_case.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view"
    response = requests.get(url, headers)

    if response.status_code == 401:
        print("Token expired, refreshing token...")
        bearer_token = get_bearer_token()
        headers['Authorization'] = f'Bearer {bearer_token}'
        response = requests.get(url, headers=headers)

    if response.status_code == 200:
        filename = f"{sys_id}.pdf"
        file_path = os.path.join(pdf_dir, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"PDF successfully saved as {file_path}")
    else:
        print(f"Failed to download PDF. Status code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download and export pdf from ServiceNow')
    parser.add_argument('sys_id', type=str, help='sys_id (e.g., HRxxxxxxxxxx)')
    args = parser.parse_args()
    sys_id = args.sys_id
    
    # Create nested folder structure
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    main_folder = f"HR_{timestamp}"
    sys_id_folder = os.path.join(main_folder, sys_id)
    attachments_folder = os.path.join(sys_id_folder, "attachments")
    pdfs_folder = os.path.join(sys_id_folder, "PDFs")
    
    os.makedirs(attachments_folder, exist_ok=True)
    os.makedirs(pdfs_folder, exist_ok=True)
    
    download_servicenow_pdf(sys_id, pdfs_folder,headers)
    download_attachments_for_article(sys_id, attachments_folder, headers)
