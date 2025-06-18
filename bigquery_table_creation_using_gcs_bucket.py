from google.cloud import storage, bigquery
import pandas as pd
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodtreasury87796-fd10b79fc8d5.json"

# Configuration
bucket_name = "treasury_tickets_demo"
main_folder = "Treasury_Tickets_20250613_0959"
project_id = "cdhnonprodtreasury87796"
dataset_id = "treasury_tickets_dataset"  # Replace with your actual dataset name
table_id = "treasury_tickets_table"

def create_dataset_if_not_exists(bq_client):
    """Create BigQuery dataset if it doesn't exist"""
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your preferred location
        bq_client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

def create_table_if_not_exists(bq_client):
    """Create BigQuery table if it doesn't exist"""
    schema = [
        bigquery.SchemaField("treasury_number", "STRING", mode="REQUIRED"),
        # bigquery.SchemaField("hr_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pdfs", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("attachments", "STRING", mode="REQUIRED")
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
    """Retrieve all HRxxx folders from GCS bucket"""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    
    hr_folders = set()
    for blob in blobs:
        path_parts = blob.name.split('/')
        # if len(path_parts) > 2 and path_parts[1].startswith("HR"):
        if len(path_parts) > 2 and path_parts[1].startswith("BIR"):    
            hr_folders.add(path_parts[1])
    return sorted(hr_folders)

def create_bigquery_table():
    # Initialize BigQuery client
    bq_client = bigquery.Client(project=project_id)
    
    # Create dataset and table if they don't exist
    create_dataset_if_not_exists(bq_client)
    create_table_if_not_exists(bq_client)
    
    # Get all HR numbers
    hr_numbers = get_hr_folders()
    
    # Create DataFrame with links
    data = []
    # base_uri = f"gs://{bucket_name}/{main_folder}"
    
    # for hr in hr_numbers:
    #     pdfs_link = f"{base_uri}/{hr}/PDFs/"
    #     attachments_link = f"{base_uri}/{hr}/Attachments/"
        
    #     data.append({
    #         "hr_number": hr,
    #         "pdfs": pdfs_link,
    #         "attachments": attachments_link
    #     })
    
    base_uri = f"gs://{bucket_name}/{main_folder}"
    for hr in hr_numbers:
        # Use HTTPS links for clickable access
        pdfs_link = f"https://storage.cloud.google.com/{bucket_name}/{main_folder}/{hr}/PDFs/"
        attachments_link = f"https://storage.cloud.google.com/{bucket_name}/{main_folder}/{hr}/Attachments/"
        
        data.append({
            "hr_number": hr,
            "pdfs": pdfs_link,
            "attachments": attachments_link
        })

    
    df = pd.DataFrame(data)
    
    # Load data
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"  # Change to WRITE_APPEND for updates
    )
    
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    job = bq_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")

if __name__ == "__main__":
    create_bigquery_table()
