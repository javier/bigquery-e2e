!/usr/bin/bash -v
#
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.
#
# Chapter 12 bash commands. The responses are in ch12.out.
# To use this script, run
# source ch12.sh
# This will set up environment variables and exit.
# To run the individual commands,  cut and paste them
# onto the command line; some of the comands require
# editing.

### To setup credentials for the examples run:
python auth.py

### Set a project ID. Substitute your project ID instead.
export PROJECT_ID=bigquery-e2e
### Set a bucket id. Substitute your own Google Cloud 
### Storage bucket instead.
export BUCKET_ID=bigquery-e2e
### AppEngine App ID.
export APP_ID=bigquery-mr-sample
### Google Cloud Storage bucket to write results to.
GCS_BUCKET=bigquery-e2e

### Quits the bash script
return

# Run an exctract job:
BASE_URL="https://www.googleapis.com/bigquery/v2"
JOBS_URL="${BASE_URL}/projects/${PROJECT_ID}/jobs"
GCS_OBJECT="data/extract/shakespeare_$(date +'%s').json"
DESTINATION_PATH="gs://${BUCKET_ID}/${GCS_OBJECT}"
SOURCE_TABLE="{ \
  'projectId': 'publicdata', \
  'datasetId': 'samples', \
  'tableId': 'shakespeare'}"
JOB_CONFIG="{'extract': { 'sourceTable': ${SOURCE_TABLE}, \
  'destinationUris': ['${DESTINATION_PATH}'], \
  'destinationFormat': 'NEWLINE_DELIMITED_JSON'}}"
JOB="{'configuration': ${JOB_CONFIG}}"
curl  \
  -H "$(python auth.py)" \
  -H "Content-Type: application/json" \
  -X POST \
  --data-binary "${JOB}" \
  "${JOBS_URL}"


# Run script locally
# Must be in the appengine directory to run the python script.
cd appengine
python add_zip.py <../add_zip_sample.json
cd ..

# Prepare GCS for AppEngine MR output.
# Note that the sample file just contains 10 rows. If you want to download the
# full file you can run:
curl -O http://commondatastorage.googleapis.com/bigquery-e2e/chapters/12/add_zip_input.json

gsutil acl ch \
    -u ${APP_ID}@appspot.gserviceaccount.com:W \
     gs://${GCS_BUCKET}

# Populate the AppEngine directory
python setup_appengine.py ${APP_ID} ${PROJECT_ID} ${GCS_BUCKET)

# Upload the app to AppEngine.
# Edit appengine/app.yaml and set the application param to your
# application id.
appcfg appengine
appcfg.py update app.yaml controller.yaml

gsutil ls gs://${GCS_BUCKET}/test/*

# Command to clean up temporary files from the mapreduce operation.
gsutil rm gs://${GCS_BUCKET}/tmp/mapreduce/**

