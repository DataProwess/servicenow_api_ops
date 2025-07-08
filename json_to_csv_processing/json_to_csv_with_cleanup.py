import json
import csv
import datetime
import re
import html
from bs4 import BeautifulSoup  # pip install beautifulsoup4

def clean_html(raw_html):
    if not raw_html:
        return ''
    # Use BeautifulSoup to parse and get text
    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    # Unescape HTML entities (like &nbsp;)
    text = html.unescape(text)
    # Replace non-breaking spaces and other whitespace chars with normal space
    text = re.sub(r'\s+', ' ', text)
    return text

input_file = "Treasury_records_combined_20250703_125727.json"
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

rows = data['result']
headers = list(rows[0].keys())

processed_rows = []
for row in rows:
    processed_row = {}
    for key in headers:
        if key in row:
            val = row[key]
            # Clean 'description' field specifically
            if key == 'description' and isinstance(val, str):
                val = clean_html(val)
            # If value is nested dict with display_value
            elif isinstance(val, dict) and 'display_value' in val:
                val = val['display_value']
            processed_row[key] = val
        else:
            processed_row[key] = None
    processed_rows.append(processed_row)

timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'{input_file}_CLEANED_output_{timestamp}.csv'
with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    writer.writerow(headers)
    for row in processed_rows:
        writer.writerow([str(row.get(key, '')) for key in headers])

print(f"CSV file written: {output_file}")
