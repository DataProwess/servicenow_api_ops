import argparse
import requests
import json
import os
import time  # Uncomment the token caching section if you want to use it
from urllib.parse import urlencode
import datetime
from dotenv import load_dotenv
load_dotenv() 

# Optional: Token caching for efficiency (comment out if not needed)
token_cache = {'value': None, 'expires': 0}

def get_bearer_token():
    # Optional: Uncomment this section for token caching
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
        print(f"🔑 Token request status: {response.status_code}")
        # Optional: Uncomment for token caching
        token_cache = {
            'value': data['access_token'],
            'expires': time.time() + data['expires_in'] - 60
        }
        return data['access_token']
    except requests.exceptions.RequestException as e:
        print(f"❌ Token request failed: {str(e)}")
        raise SystemExit(1)

def fetch_json_response(limit, offset):

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
        current_datetime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"HR_response_{current_datetime}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"✅ Response saved to {filename}")
        print(f"   Records fetched: {len(data.get('result', []))}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response: {e.response.text}")
        raise SystemExit(1)
    except json.JSONDecodeError:
        print("❌ Invalid JSON response from API")
        raise SystemExit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch records from ServiceNow API')
    parser.add_argument('--limit', type=int, default=10000, help='Maximum number of records to fetch (sysparm_limit)')
    parser.add_argument('--offset', type=int, default=0, help='Starting record index (sysparm_offset)')
    args = parser.parse_args()

    if args.limit < 1 or args.offset < 0:
        parser.error("Limit must be ≥1 and offset must be ≥0")

    fetch_json_response(args.limit, args.offset)
