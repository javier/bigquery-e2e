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

import sys
import threading
import time

from apiclient.errors import HttpError
# Sample code authorization support.
import auth
# Sample code job runner.
import jobrunner

def make_gcs_uri(gcs_bucket, gcs_object):
  return 'gs://%s/%s' % (gcs_bucket, gcs_object)
    
class GcsReadThread (threading.Thread):
  '''Waits for files to be written to GCS from a BigQuery extract job.'''
  def __init__(self, job_runner, partition_id):
    threading.Thread.__init__(self)
    self.job_runner = job_runner
    self.partition_id = partition_id
    self.gcs_service = auth.build_gcs_client()
    self.gcs_bucket = job_runner.gcs_bucket
    self.gcs_object_glob = None

  def resolve_shard_path(self, path, index):
    '''Turns a glob path and an index into the expected filename.'''
    path_fmt = path.replace('*', '%012d')
    return path_fmt % (index,)

  def check_gcs_file(self, gcs_object):
    '''Returns a tuple of GCS file URI, size if the file is present.'''
    try:
      metadata = self.gcs_service.objects().get(
          bucket=self.gcs_bucket, object=gcs_object).execute()
      uri = make_gcs_uri(self.gcs_bucket, gcs_object)
      return (uri, int(metadata.get('size', 0)))
    except HttpError, err:
      # If the error is anything except a 'Not Found' print the error.
      if err.resp.status <> 404:
        print err
      return (None, None)

  def check_gcs_shard(self, shard):
    '''Returns the URI if a sharded file is present or None.'''
    resolved_object = self.resolve_shard_path(self.gcs_object_glob, shard)
    return self.check_gcs_file(resolved_object)

  def start(self, gcs_object_glob):
    ''' Starts the thread, operating on a particular gcs object pattern.'''
    self.gcs_object_glob = gcs_object_glob;
    threading.Thread.start(self)

  def wait_for_complete(self):
    ''' Waits for the thread to complete.'''
    self.join()

  def run(self):
    '''Waits for files to be written and prints their URI when they arrive.'''
    
    if not self.gcs_object_glob: 
       raise Exception('Must set the gcs_object_glob before starting the thread')

    print "[%d] STARTING on %s" % (self.partition_id,
        make_gcs_uri(self.gcs_bucket, self.gcs_object_glob))
    result_uris = []
    job_done = False
    while True:
      shard_uri, file_size = self.check_gcs_shard(len(result_uris))
      if shard_uri: 
        # Found a new file, save it, and start looking for the next one.
        result_uris.append(shard_uri)
        print '[%d] %s size: %d' % (self.partition_id, shard_uri, file_size)
      elif job_done: break
      else:
        # Check whether the job is done. If the job is done, we don't
        # want to exit immediately; we want to check one more time
        # for files.
        job_done = self.job_runner.get_job_state() == 'DONE'
        if not job_done:
          # Didn't find a new path, and the job is still running,
          # so wait a few seconds and try again.
          time.sleep(5)
    print "[%d] DONE" % (self.partition_id,)


class TableExporterPartitioned(jobrunner.JobRunner):
  '''Class that runs an export job and processes results in multiple threads.'''
  def __init__(self, project_id, gcs_bucket):
    jobrunner.JobRunner.__init__(self, project_id) 
    self.gcs_bucket = gcs_bucket

  def make_extract_config(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      destination_uris):
    '''Creates a dict containing an export job configuration.'''

    source_table_ref = {
        'projectId': source_project_id,
        'datasetId': source_dataset_id,
        'tableId': source_table_id}
    extract_config = {
        'sourceTable': source_table_ref,
        'destionationFormat': 'NEWLINE_DELIMITED_JSON',
        'destinationUris': destination_uris}
    return {'extract': extract_config}

  def run_partitioned_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      partition_readers):
    '''Runs a BigQuery extract job and verifies the results are produced.'''
    destination_uris = []
    gcs_objects = []
    timestamp = int(time.time())
    for index in range(len(partition_readers)):
      gcs_object = 'output/%s.%s_%d.%d.*.json' % (
          source_dataset_id, 
          source_table_id,
          timestamp,
          index)
      gcs_objects.append(gcs_object)
      destination_uris.append(make_gcs_uri(self.gcs_bucket, gcs_object))
    job_config = self.make_extract_config(
        source_project_id,
        source_dataset_id,
        source_table_id,
        destination_uris)
    if not self.start_job(job_config):
      return

    for index in range(len(partition_readers)):
       partition_readers[index].start(gcs_objects[index])    
    for index in range(len(partition_readers)):
       partition_readers[index].wait_for_complete()
    self.wait_for_complete()
  

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
  exporter = TableExporterPartitioned(
      project_id=argv[0],
      gcs_bucket=argv[4])

  partition_count = int(argv[5])
  readers = []
  for index in range(partition_count):
    readers.append(GcsReadThread(job_runner=exporter, 
                                 partition_id=index))
  exporter.run_partitioned_extract_job(
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3],
      partition_readers=readers)

if __name__ == "__main__":
    main(sys.argv[1:])

