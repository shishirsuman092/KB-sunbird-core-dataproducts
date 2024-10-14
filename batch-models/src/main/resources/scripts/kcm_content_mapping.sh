#!/bin/bash

# Set your project ID
PROJECT_ID="prj-kb-poc1-bigquery-gcp-1009"

# Set the dataset name
DATASET_NAME="kb_avro_dataset_01"

# cloud storage folder name
STORAGE_FOLDER="karamyogi-bq-poc-01"

# delete all existing avro files
gsutil rm -a gs://karamyogi-bq-poc-01/kcm_content_mapping/*.avro

# delete all data from bq table
bq query --nouse_legacy_sql 'DELETE FROM `prj-kb-poc1-bigquery-gcp-1009.kb_avro_dataset_01.kcm_content_mapping` WHERE 1=1'

# copy avro files to cloud storage
gsutil cp /mount/data/analytics/warehouse/kcm_content_mapping/part*.avro gs://karamyogi-bq-poc-01/kcm_content_mapping/

# create empty schema
#bq --location=asia-south1 load --noreplace --autodetect --schema_update_option=ALLOW_FIELD_ADDITION --source_format=AVRO prj-kb-poc1-bigquery-gcp-1009:kb_avro_dataset_01.kcm_content_mapping gs://karamyogi-bq-poc-01/kcm_content_mapping/*.avro

# load data to table
bq load \
--source_format=AVRO \
kb_avro_dataset_01.kcm_content_mapping \
"gs://karamyogi-bq-poc-01/kcm_content_mapping/*.avro"                                             