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

class TableExporter:
  def __init__(self, bq_service, gcs_service, project_id):
    self.bq_service = bq_service
    self.gcs_service = gcs_service
    self.project_id = project_id

  def make_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      destination_uri):
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
        'destinationUri': destination_uri}
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

  def wait_for_complete(self, job_ref):
    '''Waits for a BigQuery job to complete.'''
    start = time.time()
    done = False
    while not done:
      time.sleep(5)
      result = self.bq_service.jobs().get(
          projectId=job_ref['projectId'], 
          jobId=job_ref['jobId']).execute()
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

  def check_gcs_file(self, gcs_bucket, gcs_object):
    '''Verifies that a Google Cloud Storage file is present.'''
    try: 
      metadata = self.gcs_service.objects().get(
          bucket=gcs_bucket, object=gcs_object).execute()
      print 'Got gs://%s/%s: %s' % (gcs_bucket, gcs_object, metadata)
      return True
    except HttpError, err:
      # If the error is anything except a 'Not Found' print the error.
      if err.resp.status <> 404:
        print err
      return False

  def run_extract_job(
      self,
      source_project_id,
      source_dataset_id,
      source_table_id,
      gcs_bucket):
    '''Runs a BigQuery extract job and verifies the results are produced.'''
    gcs_object = 'output/%s.%s_%d' % (source_dataset_id, source_table_id,
                                     int(time.time()))
    destination_uri = 'gs://%s/%s' % (gcs_bucket, gcs_object)
    job_body = self.make_extract_job(
        source_project_id,
        source_dataset_id,
        source_table_id,
        destination_uri)
    job_ref = self.start_job(job_body)
    if not job_ref: return
    if not self.wait_for_complete(job_ref): return
    self.check_gcs_file(gcs_bucket, gcs_object)
    print 'Read %s from GCS' % (destination_uri,)

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
  exporter = TableExporter(
      auth.build_bq_client(),
      auth.build_gcs_client(),
      argv[0])
  
  exporter.run_extract_job(
      source_project_id=argv[1],
      source_dataset_id=argv[2],
      source_table_id=argv[3],
      gcs_bucket=argv[4])

if __name__ == "__main__":
    main(sys.argv[1:])
