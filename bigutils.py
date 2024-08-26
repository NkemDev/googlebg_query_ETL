from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
import logging
import json
import io


def create_dataset(client:bigquery.Client,dataset_id:str)-> None:
    try:
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} created successfully")
    except Conflict:
        logging.info("Dataset already exists, exiting as successful")
    except Exception as e:
        logging.error(f"Encountered issue creating the Dataset{dataset_id}:{e}")

def create_table(client:bigquery.Client,project_id:str,dataset_id:str,table_id:str,schema_path:str)->None:
    #read schema
    try:
        with open(schema_path,'r') as f:
            metadata=f.read()
            schema =json.loads(metadata)
        # construct the table reference and table object
        table_ref =f"{project_id}.{dataset_id}.{table_id}"
        table =bigquery.Table(table_ref,schema=schema)
        client.create_table(table)
        logging.info(f"Table{table_id} created successfully")
    except Conflict:
        logging.info(f"{table_id}Table already exists, exiting successfully")
    except Exception as e:
        logging.error(f"Encountered issue creating the table {table_id}:{e}")

def load_csv_to_bigquery(client:bigquery.Client, dataset_id:str, table_id:str, csv_file_path:str):
    # mime_type, _ = mimetypes.guess_type(csv_file_path)
    # if mime_type != 'text/csv':
    #     print(f"The file {csv_file_path} is not a CSV file.")
    #     return
    
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    if table.num_rows > 0:
        print(f"Table {dataset_id}:{table_id} already contains data.")
        return
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False
    )
    with open(csv_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    print(f"Starting job {load_job.job_id}")
    load_job.result()
    print(f"Job finished. Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")

def load_gcs_to_bigquery(client:bigquery.Client, dataset_id:str, table_id:str, gcs_uri:str):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    if table.num_rows > 0:
        print(f"Table {dataset_id}:{table_id} already contains data.")
        return
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True
    )
    
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

    print(f"Starting job {load_job.job_id}")
    load_job.result()
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows from {gcs_uri} into {table_ref.table_id}.")

def write_to_gcs_bucket(client,bucket,file_name,file_buffer,content_type='application/json'):
    bucket_obj =client.bucket(bucket)
    blob = bucket_obj.blob(file_name)
    # Check if the blob already exists in the bucket
    if blob.exists():
        print(f"File {file_name} already exists in bucket {bucket}.")
        return f"gs://{bucket}/{file_name}"
    if isinstance(file_buffer,(io.StringIO, io.BytesIO)):
        data_bytes =file_buffer.getvalue()
    else:
        data_bytes = file_buffer.encode('utf-8')
    blob.upload_from_string(data_bytes,content_type=content_type)
    print(f"Successfully wrote data to {file_name}")
    return f"gs://{bucket}/{file_name}"

