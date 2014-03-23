# This file contains the terminal commands discussed in the Chapter 14.
# It is not intended to be executed. Use it to copy commands into
# your terminal and edit them as appropriate.

echo 'This part of the script will be skipped when sourced'
exit

# Set up logging on a GCS bucket.
# Create the log storage bucket if required. It is valid to use your
# primary bucket for log storage.
# Sample GCS log files have been made publicly available in the
# samples project bucket. However, the GCS adminstration commands
# below will not work with this project since it cannot be modified.
# You will need to modify the bucket variables to reference your
# buckets if you want to test them.
LOG_BUCKET='bigquery-e2e'
gsutil mb gs://${LOG_BUCKET}
# Set an ACL to allow logs to be written.
gsutil acl ch -g cloud-storage-analytics@google.com:W gs://${LOG_BUCKET}
# Configure logging on your primary bucket.
LOG_PREFIX='chapters/14/log'
SERVING_BUCKET='my-serving-bucket'
gsutil logging set on \
  -b gs://${LOG_BUCKET} \
  -o ${LOG_BUCKET} \
  gs://${SERVING_BUCKET}

# Fetch the reference schema to a local directory.
gsutil cp gs://pub/cloud_storage_usage_schema_v0.json /tmp/

# Load a set of logs into a BigQuery table.
LOG_DATASET='ch14'
bq mk ch14
bq load \
  --skip_leading_rows=1 \
  --schema=/tmp/cloud_storage_usage_schema_v0.json \
  ${LOG_DATASET}.gcs_usage \
  "gs://${LOG_BUCKET}/${LOG_PREFIX}_usage_2014_02_*"
