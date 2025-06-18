import requests
from urllib.parse import urlencode
import argparse
from dotenv import load_dotenv
import os

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

    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.text}")

    data = response.json()
    return data['access_token']


def download_servicenow_pdf(sys_id):
    bearer_token = get_bearer_token()
    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }
    # url = f"https://lendlease.service-now.com/sn_hr_core_case.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view" #for HR tickets
    url = f"https://lendlease.service-now.com/x_llusn_bankg_bi_req.do?PDF&sys_id={sys_id}&sysparm_view=Default%20view" #for finance tickets 
    response = requests.get(url, headers=headers)

    # Check if token expired (usually 401 Unauthorized)
    if response.status_code == 401:
        print("Token expired, refreshing token...")
        bearer_token = get_bearer_token()  # Refresh token
        headers['Authorization'] = f'Bearer {bearer_token}'
        response = requests.get(url, headers=headers)  # Retry request

    if response.status_code == 200:
        filename = f"{sys_id}.pdf"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"PDF successfully saved as {filename}")
    else:
        print(f"Failed to download PDF. Status code: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download and export pdf from ServiceNow')
    parser.add_argument('sys_id', type=str, help='sys_id (e.g., 01125e5a1b9b685017eeebd22a4bcb44)')
    args = parser.parse_args()
    sys_id = args.sys_id
    print(f"Downloading attachments for sys_id: {sys_id}")
 
    download_servicenow_pdf(sys_id)
