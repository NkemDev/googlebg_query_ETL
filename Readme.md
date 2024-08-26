s# Project 2: Interacting with GCS and Bigquery from Python

## Overview

This project is enables one to interact programmaticaly with Google Big Query and Google cloud bucket using Python. This project involves two actions:

1.  Writing an API to a google cloud bucket as JSON file and then loading the JSON file from the bucket into google big query.
2.  Loading a CSV file from local storage to google bigquery.

## Program Features

The following features were created programmatically

- Big Query Dataset: A Google BigQuery dataset is a container within a BigQuery project that organizes and manages tables and views. It serves as a logical grouping mechanism, providing a namespace to avoid name conflicts and a security boundary to control access.

- Google Cloud Bucket: Google Cloud Storage buckets are a scalable and secure object storage solution within the Google Cloud Platform (GCP). They allow you to store and retrieve any amount of data at any time.

- Google BigQuery Table: A Google BigQuery table is a structured dataset that resides within a Google BigQuery database. It is designed for handling and analyzing large-scale data.

## Pre-Requisites

The following are required for this project

- Google Cloud Subscription: This is to enable access the google cloud resource such as google cloud bigquery, google cloud bucket, Big query API.
- Python 3.X

## Project Structure

```
├──py_gcs_bq
    ├── data
        ├── data.csv
    ├── schema
        ├── schema_csv.json
        ├── schema_gcs.json

    ├── __init__.py
    ├── bigutils.py
    ├── config.py
    ├── bq_utils.py
    ├── main.py
    ├── .env
    ├──schema_gen.py
    ├── readme.md



```

## Google Authentication `.env`

This is to have access to the Google Cloud Resources

```
GOOGLE_APPLICATION_CREDENTIALS =location-of-key.json
```

## Constants keeper `config.py`

This is where the constants that will be used in this project

```
PROJECT_ID = "luminous-pier-424818-g0"
DATASET_ID = "books_etl_basics"
BUCKET_ID = "nkem-etl-basics"
TABLE_ID_CSV = "netflix_sub_data"
TABLE_ID_JSON = "goole_books_data"
BASE_URL = "https://www.googleapis.com/books/v1"
TARGET_ENDPOINT = "/volumes?q=programming"
SCHEMA_PATH_JSON = r"C:\Users\DELL\Documents\gcs_bigquery\schema\schema_gcs.json"
SCHEMA_PATH_CSV = r"C:\Users\DELL\Documents\gcs_bigquery\schema\schema_csv.json"
CSV_PATH = r"C:/Users/DELL\Documents/gcs_bigquery/data/data.csv"

```
## Schema Generation
```
import subprocess
from google.cloud import storage
from dotenv import load_dotenv
from bigquery_schema_generator.generate_schema import SchemaGenerator
import json
import logging
import os

load_dotenv()


def generate_schema_from_gcs(bucket_name, file_name, output_schema_file):
    if os.path.exists(output_schema_file):
        print(
            f"Schema file {output_schema_file} already exists. Skipping schema generation."
        )
        return

    # Initialize GCS client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Read the content of the JSON file from GCS
    json_content = blob.download_as_text()

    # Use bigquery-schema-generator to generate the schema from the JSON content
    process = subprocess.Popen(
        ["generate-schema", "--preserve_input_sort_order"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Pass the JSON content to bigquery-schema-generator
    schema_output, error = process.communicate(input=json_content.encode())

    if process.returncode == 0:
        # Write the generated schema to a file
        with open(output_schema_file, "wb") as f:
            f.write(schema_output)
        print(f"Schema generated and saved as {output_schema_file}")
    else:
        print("Error generating schema:", error.decode())


# Example usage
# generate_schema_from_gcs(
#     "nkem-etl-basics", "googleprogrammingbooks.json", "C:\Users\DELL\Documents\gcs_bigquery\schema\schema_gcs.json"
# )
def generate_schema_for_csv(fileName, outputFile):
    if os.path.exists(outputFile):
        print(f"Schema file {outputFile} already exists. Skipping schema generation.")
        return
    # Initialize the SchemaGenerator
    generator = SchemaGenerator(
        input_format="csv",
        infer_mode=True,
        quoted_values_are_strings=True,
        sanitize_names=True,
    )

    # Open the CSV file
    with open(fileName) as file:
        # Deduce the schema from the CSV content
        schema_map, errors = generator.deduce_schema(file)

    # Log any errors encountered during schema generation
    for error in errors:
        logging.info("Problem on line %s: %s", error["line_number"], error["msg"])

    # Flatten the schema to make it compatible with BigQuery
    schema = generator.flatten_schema(schema_map)

    # Save the schema to the specified output file
    with open(outputFile, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"Schema successfully generated and saved to {outputFile}")
    # generate_schema_for_csv('your_input.csv', 'schema.json')

```
## Dataset Creation

This is to create Google Big Query Dataset

```
def create_dataset(client:bigquery.Client,dataset_id:str)-> None:
    try:
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} created successfully")
    except Conflict:
        logging.info("Dataset already exists, exiting as successful")
    except Exception as e:
        logging.error(f"Encountered issue creating the Dataset{dataset_id}:{e}")
```

```
client = bigquery.Client(project=config.PROJECT_ID)

create_dataset(client=client,dataset_id=config.DATASET_ID)
```

## Google Big Query Table Creation

```
create_table(client:bigquery.Client,project_id:str,dataset_id:str,table_id:str,schema_path:str)->None:
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
```

In the `main.py` you call the function that creates the Google BigQuery Table

```
#Create table schema for the csv and json files
create_table(client=client,project_id=config.PROJECT_ID,dataset_id=config.DATASET_ID,table_id=config.TABLE_ID_CSV,schema_path=config.SCHEMA_PATH_CSV)
create_table(client=client,project_id=config.PROJECT_ID,dataset_id=config.DATASET_ID,table_id=config.TABLE_ID_JSON,schema_path=config.SCHEMA_PATH_JSON)
```

## Writing the API to GCS Bucket as json file

from the `bigutils.py`

```
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
```

The class `bq_utils.py`

```
class PlayStationAPI(BaseAPI):
    def __init__(self, base_url, headers=None):
        super().__init__(base_url, headers)

    def get_transactions(self):
        endpoint = "/games"
        return self.get(endpoint)

    @staticmethod
    def _to_jsonl_buffer(json_data):
        return '\n'.join([json.dumps(record) for record in json_data])
```

`main.py`

```
rows_to_insert =playStation.get_transactions()
jsonl_records =playStation._to_jsonl_buffer(rows_to_insert)
#write to gcs bucket
gcs_uri = write_to_gcs_bucket(client=storage_client,bucket=config.BUCKET_ID,file_name='playstation_games.json',file_buffer=jsonl_records)

```

## Load csv file to google bigquery table

```
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

```

`main.py` to load it into the bigquery table

```
#load the csv file into the table

if os.path.isfile(config.CSV_PATH):
        load_csv_to_bigquery(client=client,dataset_id=config.DATASET_ID,table_id=config.TABLE_ID_CSV,csv_file_path=config.CSV_PATH)
else:
        print(f"The file {config.CSV_PATH} does not exist.")
```

## Load GCS Bucket blob into big query table

```
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

```

`main.py` you call the function from `bigutils.py`

```
# load the json file from the gcs bucket to google big query
load_gcs_to_bigquery(client=client,dataset_id=config.DATASET_ID,table_id=config.TABLE_ID_JSON,gcs_uri=gcs_uri)

```

## Conclusion

This project involved two actions:

1. Writing the contents of an API into a GCS bucket as a JSON file and then loading the JSON file into google big query.
2. Loading a CSV file from local storage into google big query
   This involved

- Creating a new project in google cloud.
- Setting up a service account with the appropriate identity and access management roles for the project and downloading the key so as to use it for authentication
- Creation of google big query dataset
- Creation of google cloud bucket if not having one ealier(In this project you are assumed to have a bucket).
  The project was aimed to programmatically interact with Google Cloud Bucket and Big query using Python
