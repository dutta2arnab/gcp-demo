from google.cloud import storage
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\\Codes\\GCP-Config\\bq.json'

#Get os environment details
print(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

storage_client = storage.Client()
print(storage_client)

###Reading the Env variable Data

json_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

if json_path:
    try:
        with open(json_path,'r') as file:
            credentials = json.load(file)
            print(credentials)
    except FileNotFoundError:
        print('File Not Found')
    except json.JSONDecodeError:
        print('Unable to decode JSON file')
else:
    print('Env Variables not set')

#list all buckets 

bucket = storage_client.get_bucket('tdadventureworks')
blobs = bucket.list_blobs()
for blob in blobs:
    print(blob.name)
