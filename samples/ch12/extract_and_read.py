# Listing 12.1: Exporting a table via an extract job.
import json
import os
import time

from apiclient.discovery import build
from apiclient.errors import HttpError
import httplib2
# Sample code authorization support.
import auth

# Set this to your sample project id.
PROJECT_ID = 'bigquery-e2e'
SOURCE_PROJECT_ID = 'publicdata'
SOURCE_DATASET_ID = 'samples'
SOURCE_TABLE_ID = 'shakespeare'
BUCKET = 'bigquery-e2e'
OBJECT_BASE = 'extract_%s_%d' % (
  SOURCE_TABLE_ID, int(time.time()))
OBJECT_FORMAT = (OBJECT_BASE + '%04d.json')
OBJECT_PATTERN = (OBJECT_BASE + '*.json')
JOB_ID = 'ch12_%d' % int(time.time())
body = {
    'jobReference': {
        'jobId': JOB_ID
        },
    'configuration': {
        'extract': {
            'sourceTable': {
                'projectId': SOURCE_PROJECT_ID,
                'datasetId': SOURCE_DATASET_ID,
                'tableId': SOURCE_TABLE_ID 
                }
            }
        }
    }
extractConfig = body['configuration']['extract']
# Setup the job here.
# load[property] = value
# Select the JSON output format.
extractConfig['destinationFormat'] = 'NEWLINE_DELIMITED_JSON'
extractConfig['destinationUri'] = 'gs://%s/%s' % (
    BUCKET, OBJECT_PATTERN)
# End of job configuration.

http = auth.get_creds().authorize(httplib2.Http())
bq = build('bigquery', 'v2', http=http)
gcs = build('storage', 'v1beta2', http=http)
jobs = bq.jobs()
objects = gcs.objects()

# Create the job.
def StartJob(body):
  try: 
    result = jobs.insert(projectId=PROJECT_ID,
                         body=body).execute()
    print json.dumps(result, indent=2)
    return True
  except HttpError, err:
    print err
    return False

# Wait for completion.
def WaitForComplete(job_id):
  start = time.time()
  done = False
  while not done:
    time.sleep(5)
    result = jobs.get(projectId=PROJECT_ID, jobId=job_id).execute()
    print '%s %ds' % (result['status']['state'], time.time() - start)
    done = result['status']['state'] == 'DONE'

  # Print all errors and warnings.
  for err in result['status'].get('errors', []):
    print json.dumps(err, indent=2)

  # Check for failure.
  if 'errorResult' in result['status']:
    print 'JOB FAILED'
    print json.dumps(result['status']['errorResult'], indent=2)
    return False
  else:
    print 'JOB COMPLETED'
    return True

# Look for a Cloud Storage output file.
def CheckGcsFile(index):
  object = OBJECT_FORMAT % (index,)
  try: 
    metadata = objects.get(bucket=BUCKET, object=object).execute()
    print 'Got %s: %s' % (object, metadata)
    return True
  except HttpError, err:
    # If the error is anything except a 'Not Found' print the error.
    if err.resp.status <> 404:
      print err
    return False

if StartJob(body) and WaitForComplete(JOB_ID):
  index = 0
  while CheckGcsFile(index): index += 1
print "Found %d extracted files" % (index,)

