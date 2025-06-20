import json
import csv
import datetime

input_file='Treasury_records_combined_20250620_071633.json'
# Load your JSON data from response.json
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

rows = data['result']

# Collect all headers in the order of the first object
headers = list(rows[0].keys())

# Prepare rows: extract display_value from nested dicts (like closed_by)
processed_rows = []
for row in rows:
    processed_row = {}
    for key in headers:
        if key in row:
            if isinstance(row[key], dict) and 'display_value' in row[key]:
                processed_row[key] = row[key]['display_value']
            else:
                processed_row[key] = row[key]
        else:
            processed_row[key] = None
    processed_rows.append(processed_row)


timestamp= datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
# Write quoted CSV
with open(f'{input_file}_output_{timestamp}.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    writer.writerow(headers)
    for row in processed_rows:
        writer.writerow([str(row.get(key, '')) for key in headers])
