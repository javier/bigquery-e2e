#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Exports a BigQuery table to Google Cloud Storage.

This module runs a BigQuery Extract Job to export a table to
Google Cloud Storage.
Running:
  python extract_and_read.py <project_id> \
      <source_project_id> <source_dataset_id> <source_table_id> \
      <destination_bucket>
will extract the table source_project_id:source_dataset_id.source_table_id
to the google cloud storage location specified by under the destination_bucket
in Google Cloud Storage.

The extract job will run in the project specified by project_id.
'''

import json
import os
import sys
import threading
import time

from apiclient.errors import HttpError
# Sample code authorization support.
import auth

class TableExporter:
  def __init__(self, bq_service, gcs_service, project_id, gcs_bucket):
    self.bq_service = bq_service
    self.gcs_service = gcs_service
    self.project_id = project_id
    self.gcs_bucket = gcs_bucket

  def make_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      destination_uris):
    '''Creates a dict containing job configuration to insert.'''

    job_id = 'job_%d' % int(time.time())

    source_table_ref = {
        'projectId': source_project_id,
        'datasetId': source_dataset_id,
        'tableId': source_table_id}
    job_ref = {'projectId': self.project_id,
               'jobId': job_id}
    extract_config = {
        'sourceTable': source_table_ref,
        'destionationFormat': 'NEWLINE_DELIMITED_JSON',
        'destinationUris': destination_uris}
    body = {
        'jobReference': job_ref,
        'configuration': {'extract': extract_config}}
    return body

  def start_job(self, body):
    '''Given a job configuration, starts the BigQuery job.'''
    try:
      result = self.bq_service.jobs().insert(
          projectId=self.project_id,
          body=body).execute()
      print json.dumps(result, indent=2)
      return result['jobReference']
    except HttpError, err:
      print 'Error starting job %s:\n%s' % (body, err)
      return None

  def get_job(self, job_ref):
    try:
      return self.bq_service.jobs().get(
          projectId=job_ref['projectId'],
          jobId=job_ref['jobId']).execute()
    except HttpError, err:
      print 'Error looking up job %s:\n%s' % (job_ref, err)
      return None

  def get_job_state(self, job_ref):
    job = self.get_job(job_ref)
    return job['status']['state'] if job else 'ERROR'

  def wait_for_complete(self, job_ref):
    '''Waits for a BigQuery job to complete.'''
    start = time.time()
    while True:
      state = self.get_job_state(job_ref) 
      print '%s %ds' % (state, time.time() - start)
      if state == 'DONE': break
      time.sleep(5)

    # Print all errors and warnings.
    job = self.get_job(job_ref)
    for err in job['status'].get('errors', []):
      print json.dumps(err, indent=2)

    # Check for failure.
    if 'errorResult' in job.get('status', {}):
      print 'JOB FAILED'
      print json.dumps(job['status']['errorResult'], indent=2)
      return False
    else:
      print 'JOB COMPLETED'
      return True

  def resolve_shard_path(self, path, index):
    path_fmt = path.replace('*', '%012d')
    return path_fmt % (index,)

  def make_gcs_uri(self, gcs_object):
    return 'gs://%s/%s' % (self.gcs_bucket, gcs_object)
    
  def check_gcs_file(self, gcs_object):
    '''Verifies that a Google Cloud Storage file is present.'''
    try:
      metadata = self.gcs_service.objects().get(
          bucket=self.gcs_bucket, object=gcs_object).execute()
      uri = self.make_gcs_uri(gcs_object)
      print 'Found %s: %s' % (uri, metadata)
      return uri 
    except HttpError, err:
      # If the error is anything except a 'Not Found' print the error.
      if err.resp.status <> 404:
        print err
      return None

  def check_gcs_shard(self, gcs_object, shard):
    resolved_object = self.resolve_shard_path(gcs_object, shard)
    return self.check_gcs_file(resolved_object)

  def run_partitioned_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      partition_count=1):
    '''Runs a BigQuery extract job and verifies the results are produced.'''
    destination_uris = []
    gcs_objects = []
    timestamp = int(time.time())
    for index in range(partition_count):
      gcs_object = 'output/%s.%s_%d.%d.*.json' % (
          source_dataset_id, 
          source_table_id,
          timestamp,
          index)
      gcs_objects.append(gcs_object)
      destination_uris.append(self.make_gcs_uri(gcs_object))
    job_body = self.make_extract_job(
        source_project_id,
        source_dataset_id,
        source_table_id,
        destination_uris)
    job_ref = self.start_job(job_body)
    if not job_ref: return
    if not self.wait_for_complete(job_ref): return
    result_uris = []
    for index in range(partition_count):
      shard = 0
      while True: 
        shard_uri = self.check_gcs_shard(gcs_objects[index], shard)
	if not shard_uri: 
	  if shard == 0:
            print "Not found: %s" % (destination_uris[index],)
	  break
        shard = shard + 1
        result_uris.append(shard_uri)
    print 'Read %s from GCS' % (result_uris,)
  
# Listing 12.2: Exporting a table and reading multiple partitions.

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

  def old_main():
    if StartJob(body): 
      threads = []
      for partition in range(PARTITION_COUNT):
	threads.append(ReadThread(partition))
	threads[partition].start()

      WaitForComplete(JOB_ID)
      for partition in range(PARTITION_COUNT):
	threads[partition].join()


def main(argv):
  if len(argv) == 0:
     argv = ['bigquery-e2e',
             'publicdata',
             'samples',
             'shakespeare',
             'bigquery-e2e',
             '2']
  if len(argv) <> 6:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0],
                 '<project_id>',
                 '<source_project_id>',
                 '<source_dataset_id>',
                 '<source_table_id>',
                 '<destination_bucket>',
                 '<partition_count>']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return
  exporter = TableExporter(
      auth.build_bq_client(),
      auth.build_gcs_client(),
      project_id=argv[0],
      gcs_bucket=argv[4])

  exporter.run_partitioned_extract_job(
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3],
      partition_count=int(argv[5]))

if __name__ == "__main__":
    main(sys.argv[1:])

