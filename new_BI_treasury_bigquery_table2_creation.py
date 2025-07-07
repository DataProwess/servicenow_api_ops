from google.cloud import storage, bigquery
import pandas as pd
import os
import math
from datetime import datetime

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cdhnonprodtreasury87796-fd10b79fc8d5.json"

# GCP configuration
bucket_name = "treasury_tickets_demo"
main_folder = "BI_Treasury_Tickets_pdfs_BI_treasury_records_combined_20250703_174922_20250707_074243"
project_id = "cdhnonprodtreasury87796"
dataset_id = "treasury_tickets_dataset"
table_pdfs_id = "BI_Treasury_PDFs_with_size_and_name"


# Generate URL, size in KB, and filename from GCS folder
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


# Identify ticket folders
def get_hr_folders():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=main_folder + "/")
    hr_folders = set()
    for blob in blobs:
        path_parts = blob.name.split('/')
        if len(path_parts) > 2 and path_parts[1].startswith("BI"):
            hr_folders.add(path_parts[1])
    return sorted(hr_folders)


# Ensure dataset exists
def create_dataset_if_not_exists(bq_client, dataset_id):
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"✅ Dataset `{dataset_id}` already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)
        print(f"✅ Created dataset `{dataset_id}`")


# Ensure table exists
def create_table_if_not_exists(bq_client, table_id, schema):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
        print(f"✅ Table `{table_id}` already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"✅ Created table `{table_id}`")


# Main BigQuery pipeline
def create_bigquery_table():
    bq_client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(bq_client, dataset_id)

    schema_tickets = [
        bigquery.SchemaField("ticket_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pdfs", "STRING"),
        bigquery.SchemaField("size_in_KB", "FLOAT"),
        bigquery.SchemaField("filename", "STRING"),
    ]

    create_table_if_not_exists(bq_client, table_pdfs_id, schema_tickets)

    tickets_data = []
    hr_numbers = get_hr_folders()
    total_hr = len(hr_numbers)

    print(f"\n📁 Found {total_hr} HR folders to process...\n")

    for index, hr in enumerate(hr_numbers, start=1):
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

        if index % 1000 == 0 or index == total_hr:
            print(f"📦 Processed {index}/{total_hr} folders")

    df_tickets = pd.DataFrame(tickets_data)

    # Upload in chunks
    chunk_size = 100
    total_rows = len(df_tickets)
    total_chunks = math.ceil(total_rows / chunk_size)
    table_ref = bq_client.dataset(dataset_id).table(table_pdfs_id)

    print(f"\n📤 Starting upload of {total_rows} rows in {total_chunks} chunks...\n")
    start_time = datetime.now()

    for i in range(0, total_rows, chunk_size):
        chunk_index = i // chunk_size + 1
        chunk = df_tickets.iloc[i:i + chunk_size]
        disposition = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(write_disposition=disposition)

        print(f"🚚 Uploading chunk {chunk_index}/{total_chunks} "
              f"({i + 1}–{i + len(chunk)} of {total_rows})...", end=' ', flush=True)

        job = bq_client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
        job.result()

        elapsed = (datetime.now() - start_time).total_seconds()
        avg_per_chunk = elapsed / chunk_index
        est_remaining = (total_chunks - chunk_index) * avg_per_chunk

        print(f"✓ done | ⏱ Elapsed: {int(elapsed)}s | ETA: ~{int(est_remaining)}s")

    print(f"\n✅ Completed upload of all {total_rows} rows into BigQuery table `{table_pdfs_id}`")


if __name__ == "__main__":
    create_bigquery_table()
