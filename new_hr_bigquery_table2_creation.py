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
table_pdfs_id = "hr_PDFs_with_size_and_name"
table_attachments_id = "hr_Attachments_with_size_and_name"


def generate_console_urls_sizes_and_filenames(folder_path):
    """
    List blobs in the given folder path and return list of (url, size_in_KB, filename) tuples.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    url_size_name_list = []
    for blob in blobs:
        if not blob.name.endswith('/'):
            url = f"https://storage.cloud.google.com/{bucket_name}/{blob.name}?authuser=1"
            size_in_KB = blob.size / 1024  # Convert bytes to KB (binary)
            filename = blob.name.split('/')[-1]  # Extract filename from blob.name
            url_size_name_list.append((url, size_in_KB, filename))
    return url_size_name_list


def get_hr_folders():
    """
    List unique HR folders under the main folder.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    hr_folders = set()
    for blob in blobs:
        path_parts = blob.name.split('/')
        if len(path_parts) > 2 and path_parts[1].startswith("HR"):
            hr_folders.add(path_parts[1])
    return sorted(hr_folders)


def create_dataset_if_not_exists(bq_client, dataset_id):
    """
    Create BigQuery dataset if it does not exist.
    """
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")


def create_table_if_not_exists(bq_client, table_id, schema):
    """
    Create BigQuery table if it does not exist.
    """
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Created table {table_id}")


def create_bigquery_table():
    bq_client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(bq_client, dataset_id)

    # Define schemas with size_in_KB and filename columns
    schema_tickets = [
        bigquery.SchemaField("ticket_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pdfs", "STRING"),
        bigquery.SchemaField("size_in_KB", "FLOAT"),
        bigquery.SchemaField("filename", "STRING"),
    ]
    schema_attachments = [
        bigquery.SchemaField("ticket_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("attachments", "STRING"),
        bigquery.SchemaField("size_in_KB", "FLOAT"),
        bigquery.SchemaField("filename", "STRING"),
    ]

    # Create tables if not exist
    create_table_if_not_exists(bq_client, table_pdfs_id, schema_tickets)
    create_table_if_not_exists(bq_client, table_attachments_id, schema_attachments)

    # Prepare data for tickets (PDFs)
    tickets_data = []
    hr_numbers = get_hr_folders()
    for hr in hr_numbers:
        pdf_folder = f"{main_folder}/{hr}/PDFs/"
        pdf_url_size_name = generate_console_urls_sizes_and_filenames(pdf_folder)
        if pdf_url_size_name:
            pdf_url, size_in_KB, filename = pdf_url_size_name[0]
        else:
            pdf_url, size_in_KB, filename = None, None, None
        tickets_data.append({
            "ticket_number": hr,
            "pdfs": pdf_url,
            "size_in_KB": size_in_KB,
            "filename": filename
        })
    df_tickets = pd.DataFrame(tickets_data)

    # Load tickets data into BigQuery
    job = bq_client.load_table_from_dataframe(
        df_tickets,
        bq_client.dataset(dataset_id).table(table_pdfs_id),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(df_tickets)} rows into {table_pdfs_id}")

    # Prepare data for attachments
    attachments_data = []
    for hr in hr_numbers:
        attachments_folder = f"{main_folder}/{hr}/Attachments/"
        attachment_url_size_name = generate_console_urls_sizes_and_filenames(attachments_folder)
        for attachment_url, size_in_KB, filename in attachment_url_size_name:
            attachments_data.append({
                "ticket_number": hr,
                "attachments": attachment_url,
                "size_in_KB": size_in_KB,
                "filename": filename
            })
    df_attachments = pd.DataFrame(attachments_data)

    # Load attachments data into BigQuery
    job = bq_client.load_table_from_dataframe(
        df_attachments,
        bq_client.dataset(dataset_id).table(table_attachments_id),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(df_attachments)} rows into {table_attachments_id}")


if __name__ == "__main__":
    create_bigquery_table()
