#!/bin/bash

# Set your project ID
PROJECT_ID="prj-kb-poc1-bigquery-gcp-1009"

# Set the dataset name
DATASET_NAME="kb_avro_dataset_01"

# cloud storage folder name
STORAGE_FOLDER="karamyogi-bq-poc-01"

# delete all existing avro files
gsutil rm -a gs://karamyogi-bq-poc-01/assessment_detail/*.avro

# delete all data from bq table
bq query --nouse_legacy_sql 'DELETE FROM `prj-kb-poc1-bigquery-gcp-1009.kb_avro_dataset_01.assessment_detail` WHERE 1=1'

# copy avro files to cloud storage
gsutil cp /mount/data/analytics/warehouse/assessment_detail/part*.avro gs://karamyogi-bq-poc-01/assessment_detail/

# create empty schema
#bq --location=asia-south1 load --noreplace --autodetect --schema_update_option=ALLOW_FIELD_ADDITION --source_format=AVRO prj-kb-poc1-bigquery-gcp-1009:kb_avro_dataset_01.assessment_detail gs://karamyogi-bq-poc-01/assessment_detail/*.avro

# load data to table
bq load \
--source_format=AVRO \
kb_avro_dataset_01.assessment_detail \
"gs://karamyogi-bq-poc-01/assessment_detail/*.avro"                                             