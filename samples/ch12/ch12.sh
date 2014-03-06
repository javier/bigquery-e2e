# Run an exctract job:
BASE_URL=https://www.googleapis.com/bigquery/v2
JOBS_URL=${BASE_URL}/projects/bigquery-e2e/jobs
SOURCE_TABLE="{ \
  'projectId' : 'publicdata', \
  'datasetId': 'samples', \
  'tableId': 'shakespeare'}"
JOB_CONFIG="{'extract': { sourceTable': ${SOURCE_TABLE}, \
  'destinationUri': 'gs://bigquery-e2e/data/extract/shakespeare*.json', \
  'destinationFormat': 'NEWLINE_DELIMITED_JSON'}}"
JOB="{'configuration': ${JOB_CONFIG}}"
curl  \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST \
  -d "${JOB}" \
  "${JOBS_URL}"


# Run script locally
# Must be in the appengine directory to run the python script.
cd appengine
python add_zip.py <../add_zip_sample.json
cd ..
# Note that the sample file just contains 10 rows. If you want to download the
# full file you can run:
curl -O http://commondatastorage.googleapis.com/bigquery-e2e/chapters/12/add_zip_input.json

# Prepare GCS for AppEngine MR output.
gsutil acl ch \
  -u <application-id>@appspot.gserviceaccount.com:W \
  gs://bigquery-e2e

# Upload the app to AppEngine.
alias appcfg=<path to appengine sdk>/appcfg.py
# Edit appengine/app.yaml and set the application param to your
# application id.
appcfg appengine

# Check for outputs from MapReduce.
gsutil ls gs://<bucket>/test/*

# Adding the controller module.
# Edit controller.py to set the PROJECT_ID and GCS_BUCKET variables.
appcfg appengine/controller.yaml

# Command to clean up temporary files from the mapreduce operation.
gsutil rm gs://bigquery-e2e/tmp/mapreduce/**
