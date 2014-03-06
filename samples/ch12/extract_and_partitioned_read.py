# Listing 12.2: Exporting a table and reading multiple partitions.
import json
import os
import threading
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

JOB_ID = 'ch12_%d' % int(time.time())

PARTITION_COUNT = 1

def GetObjectFormat(partition, index):
  return '%s_%d_%04d.json' % (OBJECT_BASE, partition, index)

def GetObjectPattern(partition):
  return ('%s_%d_' % (OBJECT_BASE, partition)) + '*.json'

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
for partition_id in range(PARTITION_COUNT):
  # TODO(tigani): Use a list here.
  extractConfig['destinationUri'] = 'gs://%s/%s' % (
      BUCKET, GetObjectPattern(partition_id))
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


def IsJobRunning(job_id):
    result = jobs.get(projectId=PROJECT_ID, jobId=job_id).execute()
    return result['status']['state'] <> 'DONE'

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
def CheckGcsFile(partition, index):
  object = GetObjectFormat(partition, index)
  try: 
    metadata = objects.get(bucket=BUCKET, object=object).execute()
    print 'Got %s: %s' % (object, metadata)
    return metadata['size']
  except HttpError, err:
    # If the error is anything except a 'Not Found' print the error.
    if err.resp.status <> 404:
      print err
      raise
    return -1


class ReadThread (threading.Thread):
    def __init__(self, partition):
        threading.Thread.__init__(self)
        self.partition = partition
    def run(self):
        print "Starting %d " % (self.partition,)
        index = 0
        job_running = True
        while True:
          file_size = CheckGcsFile(self.partition, index)
          if file_size > 0: index += 1
          elif not job_running:
            break
          else: 
            job_running = IsJobRunning(JOB_ID)
            time.sleep(5)
        print "Exiting %d " % (self.partition,)


if StartJob(body): 
  threads = []
  for partition in range(PARTITION_COUNT):
    threads.append(ReadThread(partition))
    threads[partition].start()

  WaitForComplete(JOB_ID)
  for partition in range(PARTITION_COUNT):
    threads[partition].join()

