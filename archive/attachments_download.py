import requests
import os
import json
import argparse
from urllib.parse import urlencode
import datetime
from dotenv import load_dotenv
import re

load_dotenv()

def sanitize_filename(filename):
    # Replace or remove illegal characters
    illegal_chars = r'[<>:"/\\|?*\0]'
    filename = re.sub(illegal_chars, '_', filename)
    # Remove leading/trailing spaces and dots (Windows restrictions)
    filename = filename.strip('. ').strip()
    return filename

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
        content_type = attachment.get('content_type')
        trimmed_content_type = content_type[content_type.find('/') + 1:] if content_type else 'unknown'
        file_name = f"{sys_id}_{file_name}" if file_name else f"{table_sys_id}_attachment"
        file_name = sanitize_filename(file_name) 
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        download_link = attachment.get('download_link')
        file_size = attachment.get('size_bytes')

        if download_link and file_name:
            try:
                file_response = requests.get(download_link, headers=headers)
                if file_response.status_code == 200:
                    print("200 OK")
                    file_path = os.path.join(output_dir, file_name)
                    with open(file_path, 'wb') as f:
                        f.write(file_response.content)
                    print(f"   ✓ Downloaded: {file_name} ({file_size} bytes)")
                elif file_response.status_code == 500:
                    download_link=f"https://lendlease.service-now.com/sys_attachment.do?sys_id=61a7faf3133cef848f1a3d27d144b04d&sysparm_this_url=x_llusn_bankg_bi_req.do%3Fsys_id%3D8c842a9213ace74097ac3998d144b057%26sysparm_stack%3D%26sysparm_view%3D"
                    print(download_link)
                    
                    headers['Authorization'] = f'Bearer {get_bearer_token()}'
                    headers['Content-Type'] = f'{content_type}'
                    print(headers)
                    file_response = requests.get(download_link, headers=headers)
             
                    file_name = f"{file_name}_{timestamp}.jpg" 
             
                    file_path = os.path.join(output_dir, file_name)
             
                    with open(file_path, 'wb') as f:
                        f.write(file_response.content)
                    print(f"   ✓ Downloaded: {file_name} ({file_size} bytes)")
                
                else:
                    print(f"   ✗ Failed to download {file_name} (Status {file_response.status_code})")
            except Exception as e:
                print(f"   ✗ Error downloading {file_name}: {e}")


# Run the download function
if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description='Download and export pdf from ServiceNow')
    parser.add_argument('sys_id', type=str, help='sys_id (e.g., 01125e5a1b9b685017eeebd22a4bcb44)')
    args = parser.parse_args()
    sys_id = args.sys_id
    print(f"Downloading attachments for sys_id: {sys_id}")

    # Create timestamped folder name
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"attachments_{sys_id}_{now}"
    output_dir = os.path.join(os.getcwd(), folder_name)

    # Make the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    download_attachments_for_article(sys_id, output_dir, headers)

