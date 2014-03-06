# This file contains the terminal commands discussed in the Chapter 14.
# It is not intended to be executed. Use it to copy commands into
# your terminal and edit them as appropriate.

if false; then

echo 'This part of the script will be skipped when sourced'

# Set up logging on a GCS bucket.
# Create the log storage bucket if required. It is valid to use your
# primary bucket for log storage.
gsutil mb gs://<log storage bucket>
# Set an ACL to allow logs to be written.
gsutil acl ch -g cloud-storage-analytics@google.com:W gs://<log storage bucket>
# Configure logging on your primary bucket.
gsutil logging set on \
  -b gs://<log storage bucket> \
  -o <log file prefix>
  gs://<bucket to monitor>

# Fetch the reference schema to a local directory.
gsutil cp gs://pub/cloud_storage_usage_schema_v0.json /tmp/

# Load a set of logs into a BigQuery table.
bq load \
    --skip_leading_rows=1 \
    --schema=/tmp/cloud_storage_usage_schema_v0.json \
    <logs dataset>.usage_2013_06_18 \
    'gs://<log bucket>/<log prefix>_usage_YYYY_MM_DD_*_v0'

fi
