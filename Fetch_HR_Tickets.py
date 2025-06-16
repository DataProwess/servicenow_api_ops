import argparse
import requests
import json
import os
import time
from urllib.parse import urlencode
from dotenv import load_dotenv
import datetime

load_dotenv()

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
            'expires': time.time() + data['expires_in'] - 60
        }
        return data['access_token']
    except requests.exceptions.RequestException as e:
        print(f"❌ Token request failed: {str(e)}")
        raise SystemExit(1)

def fetch_batch(limit, offset):
    url = "https://lendlease.service-now.com/api/now/table/sn_hr_core_case"
    headers = {
        'Authorization': f'Bearer {get_bearer_token()}',
    }
    params = {
        "sysparm_display_value": "true",
        "sysparm_view": "Default view",
        "sysparm_limit": limit,
        "sysparm_offset": offset
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("result", [])
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed at offset {offset}: {e}")
        raise SystemExit(1)

def fetch_all_records(batch_size=1000, offset_increment=1001):
    start_time = time.time()

    offset = 0
    batch_num = 1
    total_records = 0
    all_results = []

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    master_folder = f"HR_Tickets_{timestamp}"
    os.makedirs(master_folder, exist_ok=True)

    while True:
        print(f"\n🔄 Fetching batch {batch_num} (offset={offset})...")
        batch = fetch_batch(batch_size, offset)
        batch_count = len(batch)

        if not batch:
            print("✅ No more records to fetch. Finished.")
            break

        # Save each batch to a separate file
        batch_filename = os.path.join(master_folder, f"records_batch_{batch_num}.json")
        with open(batch_filename, "w", encoding="utf-8") as f:
            json.dump(batch, f, indent=2)
        print(f"📄 Saved {batch_count} records to {batch_filename}")

        all_results.extend(batch)
        total_records += batch_count

        if batch_count < batch_size:
            print("ℹ️ Last batch fetched. Ending loop.")
            break

        offset += offset_increment
        batch_num += 1

    # Save combined file
    combined_filename = f"all_records_combined_{timestamp}.json"
    combined_file = os.path.join(master_folder, combined_filename)
    with open(combined_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)

    print(f"\n📁 Combined total records saved: {total_records}")
    print(f"⏱️ Total time taken: {int(minutes)} minutes, {int(seconds)} seconds")

if __name__ == "__main__":
    fetch_all_records(batch_size=1000, offset_increment=1001)
