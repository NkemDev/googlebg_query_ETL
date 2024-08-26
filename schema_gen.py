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
