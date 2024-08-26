from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound, Conflict
import logging

from dotenv import load_dotenv
import config
from bigutils import (
    create_dataset,
    create_table,
    load_csv_to_bigquery,
    load_gcs_to_bigquery,
    write_to_gcs_bucket,
)
from bq_utils import GoogleBooksAPI
import os
from schema_gen import generate_schema_from_gcs, generate_schema_for_csv


logging.basicConfig(level=logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")
load_dotenv()
client = bigquery.Client(project=config.PROJECT_ID)

googleBooks = GoogleBooksAPI(base_url=config.BASE_URL)
rows_to_insert = googleBooks.get_transactions()
jsonl_records = googleBooks._to_jsonl_buffer(rows_to_insert)
storage_client = storage.Client(project=config.PROJECT_ID)

# write to gcs bucket
gcs_uri = write_to_gcs_bucket(
    client=storage_client,
    bucket=config.BUCKET_ID,
    file_name="googleprogrammingbooks.json",
    file_buffer=jsonl_records,
)

# Create schema for the two files #data.csv and the 'googleprogrammingbooks.json'
generate_schema_from_gcs(
    bucket_name=config.BUCKET_ID,
    file_name="googleprogrammingbooks.json",
    output_schema_file=config.SCHEMA_PATH_JSON,
)
generate_schema_for_csv(
    fileName=r"C:\Users\DELL\Documents\gcs_bigquery\data\data.csv",
    outputFile=config.SCHEMA_PATH_CSV,
)

# Create dataset ID for the google cloud big query
create_dataset(client=client, dataset_id=config.DATASET_ID)

# Create table schema for the csv and json files
create_table(
    client=client,
    project_id=config.PROJECT_ID,
    dataset_id=config.DATASET_ID,
    table_id=config.TABLE_ID_CSV,
    schema_path=config.SCHEMA_PATH_CSV,
)
create_table(
    client=client,
    project_id=config.PROJECT_ID,
    dataset_id=config.DATASET_ID,
    table_id=config.TABLE_ID_JSON,
    schema_path=config.SCHEMA_PATH_JSON,
)

# load the csv file into the table

if os.path.isfile(config.CSV_PATH):
    load_csv_to_bigquery(
        client=client,
        dataset_id=config.DATASET_ID,
        table_id=config.TABLE_ID_CSV,
        csv_file_path=config.CSV_PATH,
    )
else:
    print(f"The file {config.CSV_PATH} does not exist.")


# load the json file from the gcs bucket to google big query
load_gcs_to_bigquery(
    client=client,
    dataset_id=config.DATASET_ID,
    table_id=config.TABLE_ID_JSON,
    gcs_uri=gcs_uri,
)
