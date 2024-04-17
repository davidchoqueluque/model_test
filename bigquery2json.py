from google.cloud import bigquery
import json

PROJECT = 'rs-nprd-dlk-ia-dev-aif-d3d9'
DATASET = 'tmp'
TABLE = 'cce_base_dev'
# Initialize the BigQuery client
client = bigquery.Client()

# Construct a reference to your table
table_ref = client.dataset(DATASET).table(TABLE)

# Fetch the table schema
table = client.get_table(table_ref)

# Run the query and fetch results
query_job = client.query(f'SELECT * FROM {PROJECT}.{DATASET}.{TABLE}')
rows = query_job.result()

# Convert the rows to a list of dictionaries with the correct data types
rows_dict = [dict(row) for row in rows]

# Output the results to a JSON file
with open('output.json', 'w') as f:
    json.dump(rows_dict, f)
