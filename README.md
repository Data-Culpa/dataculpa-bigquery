# dataculpa-bigquery
Google BigQuery connector for Data Culpa - monitor data quality in Google BigQuery automatically with Data Culpa Validator


## Install

1. Clone the repo (or just ```dc-bigquery.py```)
2. Install python dependencies (python3):
```
pip install dataculpa-client
```
3. Create a BigQuery service account and access key; the connector will checkGOOGLE_APPLICATION_CREDENTIALS for the path to the access key JSON file. The BigQuery user will need the usual permissions to read from your desired data sets. 


## Configure


4. Run ```dc-bigquery.py --init example.yaml``` to generate a template yaml to fill in connection coordinates. The yaml will never contain secrets and is safe to put into source control. 

5. Once you have your yaml file edited, run ```dc-bigquery.py --test example.yaml``` to test the connections to BigQuery and permissions and the Data Culpa Validator controller.


## Invocation

Data ingest into Data Culpa Validator happens when calling ```dc-bigquery.py --run example.yaml```.

The ```dc-bigquery.py``` script is intended to be invoked from cron or other orchestration systems. You can run it as frequently as you wish; you can spread out instances to isolate collections or different data sets with different yaml configuration files. 

## Support and Future Improvements

There are many improvements we are considering for this module. You can get in touch by writing to hello@dataculpa.com or opening issues in this repository.


