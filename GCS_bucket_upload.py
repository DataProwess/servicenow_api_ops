from pathlib import Path
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodtreasury87796-fd10b79fc8d5.json"

def upload_directory_to_gcs(bucket_name, source_directory):
    directory = Path(source_directory)
    if not directory.exists():
        print(f"Directory '{source_directory}' does not exist.")
        return
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_folder_name = directory.name  # Get the last part of the path
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            # Prepend the source folder name to the blob path
            blob_path = f"{source_folder_name}/{file_path.relative_to(directory).as_posix()}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(str(file_path))
            print(f"Uploaded {file_path} to {blob_path}")

# Example usage:

upload_directory_to_gcs("treasury_tickets_demo", "Treasury_Tickets_20250613_0959")
