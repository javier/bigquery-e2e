#!/usr/bin/bash -v
#
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.
#
# Chapter 5 bash commands. The responses are in ch05.out.
# To use this script, run 
# source ch05.sh
# This will set up environment variables and exit.
# To run the individual commands,  cut and paste them 
# onto the command line; some of the comands require
# editing (such as setting an ETag).

### To setup credentials for the examples run:
python auth.py

### Set a project ID. Substitute your project ID instead.
export PROJECT_ID=bigquery-e2e

### Setup some handy shortcuts.
export BASE_URL=https://www.googleapis.com/bigquery/v2
export PUBLIC_PROJECT_URL=${BASE_URL}/projects/publicdata
export PUBLIC_DATASET_URL=${PUBLIC_PROJECT_URL}/datasets/samples
export PUBLIC_TABLE_URL=${PUBLIC_DATASET_URL}/tables/shakespeare

export PROJECTS_URL=${BASE_URL}/projects
export PROJECT_URL=${PROJECTS_URL}/${PROJECT_ID}
export DATASETS_URL=${PROJECT_URL}/datasets
export JOBS_URL=${PROJECT_URL}/jobs

export DATASET_URL=${DATASETS_URL}/scratch
export TABLES_URL=${DATASET_URL}/tables
export TABLE_URL=${TABLES_URL}/table1
export TABLEDATA_URL=${TABLE_URL}/data

### Setup a clean scratch dataset.
bq --project_id=${PROJECT_ID} \
    rm -f -r -d scratch
bq --project_id=${PROJECT_ID} \
    mk -d scratch

### Quits the bash script
return

### List projects to test out auth.
curl -H \
  "$(python auth.py) " \
  ${PROJECTS_URL}?alt=json

### Read the newly creatd scratch dataset
curl -H "$(python auth.py)"\
    "${DATASET_URL}"

### Set maxResults. Create another dataset so there will be more than one.
bq --project_id=${PROJECT_ID} \
    mk -d scratch_2
curl -H "$(python auth.py)" \
    "${DATASETS_URL}?maxResults=1"

### Using startIndex
bq query \
    --destination_table=scratch.table1 \
     --max_rows=0 \
     "select quantiles(word_count) from publicdata:samples.shakespeare"

curl -H "$(python auth.py)" \
    "${TABLEDATA_URL}?maxResults=1&startIndex=99"

### Using a pageToken
curl -H "$(python auth.py)" \
    "${TABLEDATA_URL}?maxResults=1"

### User input: replace the page toke below with the page token
### from the previous command.
PAGE_TOKEN="1@1376284335714"
curl -H "$(python auth.py)" \
    "${TABLEDATA_URL}?maxResults=1&pageToken=${PAGE_TOKEN}"

### Using Projections
curl -H "$(python auth.py)" \
    "${JOBS_URL}?maxResults=1&projection=minimal"

curl -H "$(python auth.py)" \
    "${JOBS_URL}?maxResults=1&projection=full"

### Applying field restrictions
curl -H "$(python auth.py)" \
   "${PROJECTS_URL}?alt=json&fields=projects(id),totalItems"

### Using ETags and the If-None-Match header
curl -H "$(python auth.py)" \
    "${TABLE_URL}?fields=etag,lastModifiedTime"

### User input: # Copy the e-tag from the response.
ETAG=\"yBc8hy8wJ370nDaoIj0ElxNcWUg/gS3ul2baST3PwOoDSGXgugy2uws\"
curl  -H "$(python auth.py)" \
    -H "If-None-Match: ${ETAG}" \
    -w "%{http_code}\\n" \
   "${TABLE_URL}?fields=etag,lastModifiedTime"

echo foo,1,1.0 >foo.csv
bq load --replace scratch.table1 foo.csv "f1:string,f2:integer,f3:float"

curl  -H "$(python auth.py)" \
    -H "If-None-Match: ${ETAG}" \
   "${TABLE_URL}?fields=etag,lastModifiedTime"

bq rm -r -f -d scratch

#### BigQuery Collections

### Projects.list()
curl -H "$(python auth.py)" \
  https://www.googleapis.com/bigquery/v2/projects?alt=json

### Datasets.insert()
DATASET_REF="{'datasetId': 'scratch', 'projectId': '${PROJECT_ID}'}"
DATASETS_URL=${PROJECT_URL}/datasets
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X POST \
    --data-binary "{'datasetReference': ${DATASET_REF}}" \
    "${DATASETS_URL}"

### Datasets.get()
DATASET_URL=${DATASETS_URL}/scratch
curl -H "$(python auth.py)" \
    "${DATASET_URL}?fields=creationTime,datasetReference(datasetId)"

### Datasets.list()
curl -H "$(python auth.py)" "${DATASETS_URL}?maxResults=1"

### Datasets.update()
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X PUT \
    --data-binary "{'datasetReference': ${DATASET_REF}, \
         'friendlyName': 'my dataset'}" \
    "${DATASETS_URL}/scratch"

### Datset.patch()
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X PATCH \
    --data-binary  "{'friendlyName': 'Bob\'s dataset'}" \
    "${DATASET_URL}"

### Datasets.delete()
curl -H "$(python auth.py)" \
    -X DELETE \
    -w "%{http_code}\\n" \
    "${DATASET_URL}"

### Tables.insert()
bq mk –d scratch
SCHEMA="{'fields': [{'name':'foo', 'type': 'STRING'}]}"
TABLE_REF="{'tableId': 'table1', \
    'datasetId': 'scratch', \
    'projectId': '${PROJECT_ID}'}"
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{'tableReference': ${TABLE_REF}, \
         'schema': ${SCHEMA}}" \
    "${TABLES_URL}"

### Tables.get()
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X GET \
    "${TABLES_URL}/table1"

### Tables.list()
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X GET \
    "${TABLES_URL}?maxResults=1"

### Tables.update()
SCHEMA2="{'fields': [ \
    {'name':'foo', 'type': 'STRING'}, \
    {'name': 'bar', 'type': 'FLOAT'}]}"
TABLE_JSON="{'tableReference': ${TABLE_REF}, 'schema': ${SCHEMA2}}"
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X PUT \
    -d "${TABLE_JSON}" \
    "${TABLES_URL}/table1"

### Tables.patch()
EXPIRATION_TIME=$(($(date +"%s")+24*60*60))000
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X PATCH \
    --data-binary "{'expirationTime': '${EXPIRATION_TIME}'}" \
    "${TABLES_URL}/table1"

### Tables.delete()
curl -H "$(python auth.py)" \
    -X DELETE \
    -w "%{http_code}\\n" \
    "${TABLES_URL}/table1"

### TableData.list()
## First create a table with soe nested data.
bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    scratch.nested nested.json \
    nested.schema.json

curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X GET \
    "${TABLES_URL}/nested/data?pp=false"

### Jobs.insert()
JOB_ID=job_$(date +"%s")
JOB_REFERENCE="{'jobId': '${JOB_ID}', 'projectId': '${PROJECT_ID}'}"
JOB_CONFIG="{'query': {'query': 'select 17'}}"
JOB="{'jobReference': ${JOB_REFERENCE}, 'configuration': ${JOB_CONFIG}}"
curl -H "$(python auth.py)"  \
    -H "Content-Type: application/json" \
    -X POST \
    -d "${JOB}" \
    "${JOBS_URL}"

### Jobs.get()
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    "${JOBS_URL}/${JOB_ID}"

### Jobs.list()
FIELDS="jobs(jobReference(jobId))"
PARAMS="stateFilter=DONE&fields=${FIELDS}&maxResults=2"
curl -H "$(python auth.py)" \
    -H "Content-Type: application/json" \
    -X GET \
    "${JOBS_URL}?${PARAMS}"

