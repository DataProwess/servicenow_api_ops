import requests
import re
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
from datetime import datetime
import logging
import time
import json
import argparse

load_dotenv()

# Create a unique timestamp for folder and files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define log folder and files with timestamp
log_dir = f"HR_STATE_CHANGE_total_logs_{timestamp}"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"general_HR_STATE_CHANGE_{timestamp}.log")

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
    error_log_file = os.path.join(log_dir, f'error_HR_STATE_CHANGE_{timestamp}.log')
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

def replace_status_values(audit_data, mapping):
    for record in audit_data.get("result", []):
        # Replace newvalue if it matches a key in mapping
        if "newvalue" in record and record["newvalue"] in mapping:
            record["newvalue"] = mapping[record["newvalue"]]
        # Replace oldvalue if it matches a key in mapping
        if "oldvalue" in record and record["oldvalue"] in mapping:
            record["oldvalue"] = mapping[record["oldvalue"]]
    return audit_data

def process_tickets_from_file(master_folder, json_filepath):
    with open(json_filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    tickets = data.get("result", [])
    # tickets = data.get("result", [])[:10]  # first 10 tickets
    combined_results = []  # List to hold all enriched audit records

    for ticket in tickets:
        ticket_number = ticket.get("number")
        ticket_sys_id = ticket.get("sys_id")  # Adjust if needed

        if not ticket_number or not ticket_sys_id:
            logging.warning(f"Skipping ticket due to missing number or ticket_sys_id: {ticket}")
            log_error_to_file(f"Skipping ticket due to missing number or ticket_sys_id: {ticket}")
            continue

        try:
            audit_data = get_sys_audit(master_folder, ticket_number, ticket_sys_id)
            audit_data = replace_status_values(audit_data, status_mapping)

            # Enrich each audit record with ticket info and add to combined list
            for record in audit_data.get("result", []):
                record["ticket_number"] = ticket_number
                record["ticket_sys_id"] = ticket_sys_id
                combined_results.append(record)

            print(f"Fetched and processed audit data for ticket {ticket_number}")

        except Exception as e:
            logging.error(f"Failed to fetch audit data for ticket {ticket_number}: {e}")
            log_error_to_file(f"Failed to fetch audit data for ticket {ticket_number}: {e}")

    # Wrap combined results inside a dict with key "result"
    final_output = {"result": combined_results}

    # Save to combined JSON file
    combined_file_path = os.path.join(master_folder, f"PROD_Combined_HR_state_change_data_{timestamp}.json")
    os.makedirs(master_folder, exist_ok=True)
    with open(combined_file_path, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=4)

    print(f"All audit data combined and saved to {combined_file_path}")



def get_sys_audit(master_folder,ticket_number, sys_id):
    url = f"https://lendlease.service-now.com/api/now/table/sys_audit?sysparm_query=tablenameLIKEsn_hr_core_case%5Edocumentkey={sys_id}^fieldnameSTARTSWITHstate"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {get_bearer_token()}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        
        os.makedirs(master_folder, exist_ok=True)  # Create folder if it doesn't exist
        file_path = os.path.join(master_folder, f"{ticket_number}_{timestamp}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return data
    else:
        raise Exception(f"API call failed with status code {response.status_code}: {response.text}")

# Example usage:
if __name__ == "__main__":
    # Path to your JSON file containing ticket data
    parser = argparse.ArgumentParser(description="Process ServiceNow tickets and fetch audit logs.")
    parser.add_argument(
        "--json_file",
        required=True,
        help="Path to the JSON file containing ticket data."
    )
    args = parser.parse_args()
    status_mapping = {
    "10": "Ready",
    "20": "Awaiting Response",
    "3":  "Closed Complete",
    "4":  "Closed Incomplete",
    "7":  "Cancelled",
    "18": "Work In Progress",
    "24": "Suspended",
    }

    json_file_path = args.json_file
    master_folder= f"PROD_HR_STATE_CHANGE_JSON_responses_{timestamp}"
    process_tickets_from_file(master_folder,json_file_path)

