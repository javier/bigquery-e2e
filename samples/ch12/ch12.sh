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

### Quits the bash script
return

# Run an exctract job:
BASE_URL=https://www.googleapis.com/bigquery/v2
JOBS_URL=${BASE_URL}/projects/${PROJECT_ID}/jobs
SOURCE_TABLE="{ \
  'projectId' : 'publicdata', \
  'datasetId': 'samples', \
  'tableId': 'shakespeare'}"
JOB_CONFIG="{'extract': { sourceTable': ${SOURCE_TABLE}, \
  'destinationUri': 'gs://${BUCKET_ID}/data/extract/shakespeare*.json', \
  'destinationFormat': 'NEWLINE_DELIMITED_JSON'}}"
JOB="{'configuration': ${JOB_CONFIG}}"
curl  \
  -H "Content-Type: application/json" \
  -X POST \
  "${JOBS_URL}"

# Run an extract job with a relative path:
JOB_CONFIG="{'extract': { sourceTable': ${SOURCE_TABLE}, \
  'destinationUri': 'shakespeare*.csv, \
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
