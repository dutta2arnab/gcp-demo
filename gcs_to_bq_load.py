from google.cloud import storage
from google.cloud import bigquery
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\\Codes\\GCP-Config\\bq.json'
print(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

def load_file_from_gcs_bq(file_bucket,file_name,bq_table_id):
    file_bucket = "arnab1-10715778-project-bucket/adventureworks"
    file_name = "BillOfMaterials_Production.csv"
    bq_table_id = "lti-coe.GCP_STG.BILLOFMATERIALS_PRODUCTION_STG"
    
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format="CSV",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    uri = f"gs://{file_bucket}/{file_name}"
    
    load_job = client.load_table_from_uri(uri,bq_table_id,job_config=job_config)
    
    load_job.result()
    
    destination_table = client.get_table(bq_table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    print(f"File {file_name} loaded into BigQuery table {bq_table_id}")
