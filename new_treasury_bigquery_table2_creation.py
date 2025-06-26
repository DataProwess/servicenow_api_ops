from google.cloud import storage, bigquery
import pandas as pd
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodtreasury87796-fd10b79fc8d5.json"

# Configuration
bucket_name = "treasury_tickets_demo"
main_folder = "Treasury_Tickets_20250613_0959"
project_id = "cdhnonprodtreasury87796"
dataset_id = "treasury_tickets_dataset"
table_id = "DEMO_treasury_gcp_table"

def create_dataset_if_not_exists(bq_client):
    """Create BigQuery dataset if it doesn't exist"""
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bq_client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

def create_table_if_not_exists(bq_client):
    """Create BigQuery table if it doesn't exist"""
    schema = [
        bigquery.SchemaField("treasury_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pdfs", "STRING", mode="REPEATED"),  # Array of URLs
        bigquery.SchemaField("attachments", "STRING", mode="REPEATED")  # Array of URLs
    ]
    
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Created table {table_id}")

def get_hr_folders():
    """Retrieve all BIRxxx folders from GCS bucket"""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    
    hr_folders = set()
    for blob in blobs:
        path_parts = blob.name.split('/')
        if len(path_parts) > 2 and path_parts[1].startswith("BIR"):
            hr_folders.add(path_parts[1])
    return sorted(hr_folders)

def generate_console_urls(folder_path):
    """Generate console-style authenticated URLs for files in a GCS folder"""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    
    urls = []
    for blob in blobs:
        if not blob.name.endswith('/'):  # Skip directory markers
            # Construct console-style URL
            url = f"https://storage.cloud.google.com/{bucket_name}/{blob.name}"
            urls.append(url)
    return urls

def create_bigquery_table():
    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    
    # Create dataset and table
    create_dataset_if_not_exists(bq_client)
    create_table_if_not_exists(bq_client)
    
    # Get all ticket numbers
    hr_numbers = get_hr_folders()
    
    # Collect data with console URLs
    data = []
    for hr in hr_numbers:
        # Generate console URLs for PDFs
        pdf_folder = f"{main_folder}/{hr}/PDFs/"
        pdf_urls = generate_console_urls(pdf_folder)
        
        # Generate console URLs for Attachments
        attachments_folder = f"{main_folder}/{hr}/Attachments/"
        attachment_urls = generate_console_urls(attachments_folder)
        
        data.append({
            "treasury_number": hr,
            "pdfs": pdf_urls,
            "attachments": attachment_urls
        })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("treasury_number", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pdfs", "STRING", mode="REPEATED"),
            bigquery.SchemaField("attachments", "STRING", mode="REPEATED")
        ]
    )
    
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    job = bq_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()
    print(f"Loaded {len(df)} rows with console URLs into {table_id}")

if __name__ == "__main__":
    create_bigquery_table()
