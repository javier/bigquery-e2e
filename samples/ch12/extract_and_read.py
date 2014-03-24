#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Exports a BigQuery table to Google Cloud Storage.

This module runs a BigQuery Extract Job to export a table to 
Google Cloud Storage.
Running:
  python extract_and_read.py <project_id> \
      <source_project_id> <source_dataset_id> <source_table_id> \
      <destination_bucket> [destination_directory]
will extract the table source_project_id:source_dataset_id.source_table_id
to the google cloud storage location specified by under the destination_bucket
in Google Cloud Storage. If destination directory is provided, will download
the results to that directory.

The extract job will run in the project specified by project_id.
'''

import json
import logging
import sys
import time

# Imports from local files in this directory:
import auth
from gcs_reader import GcsReader
from job_runner import JobRunner

def make_extract_config(source_project_id, source_dataset_id,
                        source_table_id, destination_uris):
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

def run_extract_job(job_runner, gcs_reader, source_project_id,  
    source_dataset_id, source_table_id):
  '''Runs a BigQuery extract job and reads the results.'''

  timestamp = int(time.time())
  gcs_object = 'output/%s.%s_%d.json' % (
      source_dataset_id,
      source_table_id,
      timestamp)
  destination_uri = gcs_reader.make_uri(gcs_object)
  job_config = make_extract_config(
      source_project_id,
      source_dataset_id,
      source_table_id,
      [destination_uri])
  if not job_runner.start_job(job_config):
    return
  
  print json.dumps(job_runner.get_job(), indent=2)

  job_runner.wait_for_complete()
  gcs_reader.read(gcs_object)


def main(argv):
  logging.basicConfig()
  if len(argv) == 0:
     # Sample args used in the book. You will likely not
     # have access to create jobs in the bigquery-e2e project
     # or write data to the bigquery-e2e GCS bucket.
     argv = ['bigquery-e2e',
             'publicdata',
             'samples',
             'shakespeare',
             'bigquery-e2e',
             '/tmp/bigquery']
  if len(argv) < 5:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0], 
                 '<project_id>',
                 '<source_project_id>',
                 '<source_dataset_id>',
                 '<source_table_id>',
                 '<destination_bucket>',
                 '[output_directory]']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return

  download_dir = argv[5] if len(argv) > 5 else None
  gcs_reader = GcsReader(gcs_bucket=argv[4], 
                         download_dir=download_dir)
  job_runner = JobRunner(project_id=argv[0])
  run_extract_job(
      job_runner,
      gcs_reader,
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3])

if __name__ == "__main__":
    main(sys.argv[1:])
