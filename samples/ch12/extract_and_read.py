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
import time

from apiclient.errors import HttpError
# Sample code authorization support.
import auth
# Sample code job runner.
import jobrunner

def make_gcs_uri(gcs_bucket, gcs_object):
  return 'gs://%s/%s' % (gcs_bucket, gcs_object)


class GcsReader:
  ''' Reads file from Google Cloud Storage.
     
   Runs in parallel to a BigQuery export job, will poll GCS for
   the availability of a file matching a file pattern until
   the BigQuery job is complete.
  '''

  def __init__(self, gcs_bucket):
    self.gcs_service = auth.build_gcs_client()
    self.gcs_bucket = gcs_bucket

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

  def read(self, gcs_object):
      uri, file_size = self.check_gcs_file(gcs_object)
      # If you wanted to read the file from GCS, you could do so
      # here.
      print '%s size: %d' % (uri, file_size)


class TableExporter(jobrunner.JobRunner):
  '''Class that runs an export job and processes results.'''
  def __init__(self, project_id):
    jobrunner.JobRunner.__init__(self, project_id)
    self.gcs_service = auth.build_gcs_client()

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

  def run_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      gcs_reader):
    '''Runs a BigQuery extract job and reads the results.'''
    destination_uris = []
    gcs_objects = []
    timestamp = int(time.time())
    gcs_object = 'output/%s.%s_%d.json' % (
        source_dataset_id,
        source_table_id,
        timestamp)
    destination_uri = make_gcs_uri(gcs_reader.gcs_bucket, gcs_object)
    job_config = self.make_extract_config(
        source_project_id,
        source_dataset_id,
        source_table_id,
        [destination_uri])
    if not self.start_job(job_config):
      return

    self.wait_for_complete()
    gcs_reader.read(gcs_object)


def main(argv):
  if len(argv) == 0:
     argv = ['bigquery-e2e',
             'publicdata',
             'samples',
             'shakespeare',
             'bigquery-e2e']
  if len(argv) <> 5:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0], 
                 '<project_id>',
                 '<source_project_id>',
                 '<source_dataset_id>',
                 '<source_table_id>',
                 '<destination_bucket>']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return
  exporter = TableExporter(project_id=argv[0])
  gcs_reader = GcsReader(gcs_bucket=argv[4])
  exporter.run_extract_job(
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3],
      gcs_reader=gcs_reader)

if __name__ == "__main__":
    main(sys.argv[1:])
