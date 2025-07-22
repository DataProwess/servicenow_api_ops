from google.cloud import storage, bigquery
import pandas as pd
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="cdhprodtreasury17105-SAKey-serviceNow_migration-e4f1e76fd635.json" # "cdhnonprodtreasury87796-fd10b79fc8d5.json"

# Configuration
bucket_name = "treasury_bucket_prod"
main_folder = "Treasury_Tickets_attachments_and_pdfs_Treasury_tickets_JSON_responses_20250703_125727"
project_id = "cdhprodtreasury17105"
dataset_id = "treasury_tickets_dataset"

# Table names
table_pdfs_id = "PROD_Treasury_PDFs_with_size_and_name"
table_attachments_id = "PROD_Treasury_Attachments_with_size_and_name"


def generate_console_urls_sizes_and_filenames(folder_path):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    url_size_name_list = []
    for blob in blobs:
        if not blob.name.endswith('/'):
            url = f"https://storage.cloud.google.com/{bucket_name}/{blob.name}?authuser=1"
            size_in_KB = blob.size / 1024
            filename = blob.name.split('/')[-1]
            url_size_name_list.append((url, size_in_KB, filename))
    return url_size_name_list


def get_bir_folders_with_batches():
    """
    Return list of (batch_name, bir_folder_name) tuples.
    E.g., ('batch_1', 'BIR0001234')
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    bir_folders = set()

    for blob in blobs:
        parts = blob.name.strip("/").split('/')
        # Expect structure: main_folder/batch/BIRxxxx/.../file
        if len(parts) >= 3 and parts[0] == main_folder and parts[2].startswith("BIR"):
            batch = parts[1]
            bir_folder = parts[2]
            bir_folders.add((batch, bir_folder))

    return sorted(bir_folders)


def create_dataset_if_not_exists(bq_client, dataset_id):
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")


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


def create_bigquery_table():
    bq_client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(bq_client, dataset_id)

    schema_pdfs = [
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

    create_table_if_not_exists(bq_client, table_pdfs_id, schema_pdfs)
    create_table_if_not_exists(bq_client, table_attachments_id, schema_attachments)

    # Get all (batch, BIR) folders
    bir_folders = get_bir_folders_with_batches()

    # PDFs
    tickets_data = []
    for batch, bir in bir_folders:
        pdf_folder = f"{main_folder}/{batch}/{bir}/PDFs/"
        pdf_url_size_name = generate_console_urls_sizes_and_filenames(pdf_folder)
        if pdf_url_size_name:
            pdf_url, size_in_KB, filename = pdf_url_size_name[0]
        else:
            pdf_url, size_in_KB, filename = None, None, None
        tickets_data.append({
            "ticket_number": bir,
            "pdfs": pdf_url,
            "size_in_KB": size_in_KB,
            "filename": filename
        })
    df_pdfs = pd.DataFrame(tickets_data)

    job = bq_client.load_table_from_dataframe(
        df_pdfs,
        bq_client.dataset(dataset_id).table(table_pdfs_id),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(df_pdfs)} rows into {table_pdfs_id}")

    # Attachments
    attachments_data = []
    for batch, bir in bir_folders:
        attachments_folder = f"{main_folder}/{batch}/{bir}/Attachments/"
        attachment_url_size_name = generate_console_urls_sizes_and_filenames(attachments_folder)
        for attachment_url, size_in_KB, filename in attachment_url_size_name:
            attachments_data.append({
                "ticket_number": bir,
                "attachments": attachment_url,
                "size_in_KB": size_in_KB,
                "filename": filename
            })
    df_attachments = pd.DataFrame(attachments_data)

    job = bq_client.load_table_from_dataframe(
        df_attachments,
        bq_client.dataset(dataset_id).table(table_attachments_id),
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(df_attachments)} rows into {table_attachments_id}")


if __name__ == "__main__":
    create_bigquery_table()
