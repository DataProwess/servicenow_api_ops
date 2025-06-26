from google.cloud import storage, bigquery
import pandas as pd
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodpnc44829-1296a3a1e57c.json"

# Configuration
bucket_name = "demo_hr_bucket"
main_folder = "demo_hr_upload"
project_id = "cdhnonprodpnc44829"
dataset_id = "hr_tickets_dataset"
table_id = "DEMO_hr_gcp_table"


def generate_console_urls(folder_path):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    urls = []
    for blob in blobs:
        if not blob.name.endswith('/'):
            url = f"https://storage.cloud.google.com/{bucket_name}/{blob.name}?authuser=1"
            urls.append(url)
    return urls

def get_hr_folders():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    hr_folders = set()
    for blob in blobs:
        path_parts = blob.name.split('/')
        if len(path_parts) > 2 and path_parts[1].startswith("BIR"):
            hr_folders.add(path_parts[1])
    return sorted(hr_folders)

def create_table_if_not_exists(bq_client, table_id, schema):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Created table {table_id}")
    # Note: Fix typo in above line: change bq_client to bq_client

def create_bigquery_table():
    bq_client = bigquery.Client(project=project_id)
    # Create dataset if not exists (not shown here for brevity, but use same code as before)
    # Define schemas
    schema_tickets = [
        bigquery.SchemaField("ticket_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("PDFs", "STRING")
    ]
    schema_attachments = [
        bigquery.SchemaField("ticket_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Attachments", "STRING")
    ]
    # Create tables
    create_table_if_not_exists(bq_client, "hr_PDFs", schema_tickets)
    create_table_if_not_exists(bq_client, "hr_Attachments", schema_attachments)  # Fix: use bq_client
    # Populate tickets
    tickets_data = []
    hr_numbers = get_hr_folders()
    for hr in hr_numbers:
        pdf_folder = f"{main_folder}/{hr}/PDFs/"
        pdf_urls = generate_console_urls(pdf_folder)
        pdf_url = pdf_urls[0] if pdf_urls else None
        tickets_data.append({"ticket_number": hr, "pdfs": pdf_url})
    df_tickets = pd.DataFrame(tickets_data)
    job = bq_client.load_table_from_dataframe(
        df_tickets, bq_client.dataset(dataset_id).table("hr_PDFs"), 
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    # Populate attachments
    attachments_data = []
    for hr in hr_numbers:
        attachments_folder = f"{main_folder}/{hr}/Attachments/"
        attachment_urls = generate_console_urls(attachments_folder)
        for attachment_url in attachment_urls:
            attachments_data.append({"ticket_number": hr, "attachments": attachment_url})
    df_attachments = pd.DataFrame(attachments_data)
    job = bq_client.load_table_from_dataframe(
        df_attachments, bq_client.dataset(dataset_id).table("hr_Attachments"), 
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()
    print("Data loaded into tickets and attachments tables")

if __name__ == "__main__":
    create_bigquery_table()
