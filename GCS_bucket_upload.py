from pathlib import Path
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import logging
import datetime

# Setup credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodtreasury87796-fd10b79fc8d5.json" #"cdhnonprodpnc44829-1296a3a1e57c.json"

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# Setup logging
logging.basicConfig(filename=f'upload_failures_{timestamp}.log', level=logging.ERROR, filemode='w')  

MAX_WORKERS = 16
CHUNK_SIZE_MB = 10
RETRIES = 3

# Track total size
total_uploaded_bytes = 0
total_files_uploaded = 0

def safe_upload(bucket, base_path, folder_name, file_path):
    global total_uploaded_bytes, total_files_uploaded
    blob_path = f"{folder_name}/{file_path.relative_to(base_path).as_posix()}"
    blob = bucket.blob(blob_path)
    blob.chunk_size = CHUNK_SIZE_MB * 1024 * 1024

    for attempt in range(1, RETRIES + 1):
        try:
            blob.upload_from_filename(str(file_path))
            file_size = os.path.getsize(file_path)
            total_uploaded_bytes += file_size
            total_files_uploaded += 1
            print(f"✅ Uploaded {file_path} ({file_size / (1024*1024):.2f} MB)")
            return
        except GoogleAPIError as e:
            print(f"❌ Attempt {attempt} failed for {file_path}: {e}")
            time.sleep(2 ** attempt)

    logging.error(str(file_path))  # Log failed file path only
    print(f"🚨 Giving up on {file_path}")

def upload_directory_to_gcs(bucket_name, source_directory):
    global total_uploaded_bytes, total_files_uploaded
    base_path = Path(source_directory)
    if not base_path.exists():
        print(f"❌ Directory '{source_directory}' does not exist.")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    folder_name = base_path.name

    all_files = [p for p in base_path.rglob("*") if p.is_file()]
    print(f"📁 Found {len(all_files)} files to upload.")

    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(safe_upload, bucket, base_path, folder_name, file)
            for file in all_files
        ]
        for future in as_completed(futures):
            future.result()

    end = time.time()
    print(f"✅ Upload finished in {(end - start) / 60:.2f} minutes.")
    print(f"📦 Total files uploaded: {total_files_uploaded}")
    print(f"📊 Total size uploaded: {total_uploaded_bytes / (1024**3):.2f} GB")

# Run main upload
bucket_name="treasury_tickets_demo"
folder_to_be_uploaded = r"D:\coding\servicenow_api_ops\anish_demo_treasury_upload"
upload_directory_to_gcs(bucket_name, folder_to_be_uploaded)
