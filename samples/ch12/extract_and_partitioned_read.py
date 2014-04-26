#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Exports a BigQuery table to Google Cloud Storage.

This module runs a BigQuery Extract Job to export a table to
Google Cloud Storage.
Running:
  python extract_and_partitioned_read.py <project_id> \
      <source_project_id> <source_dataset_id> <source_table_id> \
      <destination_bucket> <partitiont_count> [destination_dir]
will run a BigQuery job to extract the table:
source_project_id:source_dataset_id.source_table_id
to the Google Cloud Storage location specified by under the
destination_bucket. The job  will instruct BigQuery to extract
in partition_count partitions, and it will read those partitioned
files in parallel threads.

If destination_dir is specified, will download the results to that
directory, otherwise will just report the presence of the output files
in GCS.

The extract job will run in the project specified by project_id.
'''

import sys
import threading
import time

from apiclient.errors import HttpError

# Imports from local files in this directory:
from gcs_reader import GcsReader
from job_runner import JobRunner

class PartitionReader(threading.Thread):
  '''Reads output files from a partitioned BigQuery extract job.'''
  def __init__(self, job_runner, gcs_reader, partition_id):
    threading.Thread.__init__(self)
    self.job_runner = job_runner
    self.partition_id = partition_id
    self.gcs_reader = gcs_reader
    self.gcs_object_glob = None

  def resolve_shard_path(self, path, index):
    '''Turns a glob path and an index into the expected filename.'''
    path_fmt = path.replace('*', '%012d')
    return path_fmt % (index,)

  def read_shard(self, shard):
    '''Reads the file if the file is present or returns None.'''
    resolved_object = self.resolve_shard_path(self.gcs_object_glob,
                                              shard)
    return self.gcs_reader.read(resolved_object)

  def start(self, gcs_object_glob):
    ''' Starts the thread, reading a GCS object pattern.'''
    self.gcs_object_glob = gcs_object_glob;
    threading.Thread.start(self)

  def wait_for_complete(self):
    ''' Waits for the thread to complete.'''
    self.join()

  def run(self):
    '''Waits for files to be written and reads them when they arrive.'''

    if not self.gcs_object_glob:
      raise Exception(
          'Must set the gcs_object_glob before running thread')

    print "[%d] STARTING on %s" % (self.partition_id,
        self.gcs_reader.make_uri(self.gcs_object_glob))
    job_done = False
    shard_index = 0
    while True:
      file_size = self.read_shard(shard_index)
      if file_size is not None:
        # Found a new file, save it, and start looking for the next one.
        shard_index += 1
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
    print "[%d] DONE. Read %d files" % (self.partition_id, shard_index)


def make_extract_config(source_project_id, source_dataset_id,
                        source_table_id, destination_uris):
  '''Creates a dict containing an export job configuration.'''

  source_table_ref = {
      'projectId': source_project_id,
      'datasetId': source_dataset_id,
      'tableId': source_table_id}
  extract_config = {
      'sourceTable': source_table_ref,
      'destinationFormat': 'NEWLINE_DELIMITED_JSON',
      'destinationUris': destination_uris}
  return {'extract': extract_config}

def run_partitioned_extract_job(job_runner, gcs_readers,
    source_project_id, source_dataset_id, source_table_id):
  '''Runs a BigQuery extract job and reads the results.'''
  destination_uris = []
  gcs_objects = []
  timestamp = int(time.time())
  partition_readers = []
  for index in range(len(gcs_readers)):
    gcs_object = 'output/%s.%s_%d.%d.*.json' % (
        source_dataset_id,
        source_table_id,
        timestamp,
        index)
    gcs_objects.append(gcs_object)
    destination_uris.append(gcs_readers[index].make_uri(gcs_object))

    # Create the reader thread for this partition.
    partition_readers.append(
        PartitionReader(job_runner=job_runner,
                        gcs_reader=gcs_readers[index],
                        partition_id=index))

  job_config = make_extract_config(source_project_id, source_dataset_id,
      source_table_id, destination_uris)
  if not job_runner.start_job(job_config):
    return

  # First start all of the reader threads.
  for index in range(len(partition_readers)):
     partition_readers[index].start(gcs_objects[index])
  # Wait for all of the reader threads to complete.
  for index in range(len(partition_readers)):
     partition_readers[index].wait_for_complete()

def main(argv):
  if len(argv) == 0:
     argv = ['bigquery-e2e',
             'publicdata',
             'samples',
             'shakespeare',
             'bigquery-e2e',
             '3',
             '/tmp/bigquery']
  if len(argv) < 6:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0],
                 '<project_id>',
                 '<source_project_id>',
                 '<source_dataset_id>',
                 '<source_table_id>',
                 '<destination_bucket>',
                 '<partition_count>',
                 '[output_directory]']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return
  gcs_bucket = argv[4]
  job_runner = JobRunner(project_id=argv[0])

  partition_count = int(argv[5])
  download_dir = argv[6] if len(argv) > 6 else None
  gcs_readers = []
  for index in range(partition_count):
    # Note: a separate GCS reader is required per partition.
    gcs_reader = GcsReader(gcs_bucket=gcs_bucket,
                           download_dir=download_dir)
    gcs_readers.append(gcs_reader)

  run_partitioned_extract_job(
      job_runner,
      gcs_readers,
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3])

if __name__ == "__main__":
    main(sys.argv[1:])

