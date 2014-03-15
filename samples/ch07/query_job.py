#!/usr/bin/python2.7

import time

import auth
from apiclient.discovery import build

def run_query_job(
    service, project_id, query, response_handler, 
    job_id=None, destination_table=None, allow_large_results=False,
    batch_priority=False):

  query_config = {
      'query': query,
      'allowLargeResults': allow_large_results}

  if destination_table:
     # If this is run multiple times, append results to the destination.
    query_config['writeDisposition'] = 'WRITE_APPEND',
    query_config['destinationTable'] = destination_table
    query_config['allow_large_results'] = allow_large_results

  if batch_priority:
    query_config['prioriy'] = 'BATCH'

  job_ref = {'projectId': project_id}
  if job_id:
    job_ref['jobId'] = job_id

  job = {
      'configuration': {'query': query_config},
      'jobReference': job_ref}
      
  print 'Starting query job "%s"' % (job,)
  job = service.jobs().insert(projectId=project_id,
      body=job).execute()
  job_ref = job['jobReference']

  # Wait for the job to complete.
  while job['status']['state'] != 'DONE':
    print 'Waiting for job %s to complete: %s' % (
        job_ref, job['status']['state'])
    time.sleep(1.0)
    job = service.jobs().get(**job_ref).execute()
    
  if 'errorResult' in job['status']:
    print 'Error %s' % (job['status']['errorResult'],)
    return
    
  # Read the results using TableData.list(). Note that we could
  # also read the results using jobs.getQueryResults(), but for the 
  # purposes of this sample, we wanted to show the TableData equivalent.

  destination_table_ref = job['configuration']['query']['destinationTable']
  schema = service.tables().get(**destination_table_ref).execute()['schema']
  print 'Output schema: %s' % (schema,)
   
  page_token = None
  while True:
    response = service.tabledata().list(
        pageToken=page_token,
        **destination_table_ref).execute()
    page_token = response.get('pageToken', None)
    response_handler(response, schema)
    if page_token is None:
      # Our work is done, query is done and there are no more
      # results to read.
      break;

def print_results(results, schema):
  fields = schema['fields']
  rows = results['rows']
  for row in rows:
    for i in xrange(0, len(fields)): 
      cell = row['f'][i]
      field = fields[i]
      print "%s: %s " % (field['name'], cell['v']),
    print ''

def main():
  creds = auth.get_creds()
  service = auth.build_bq_client()
  project_id = 'bigquery-e2e'
  query = 'select * from temp.nested'
  destination = {
      'projectId': project_id,
      'datasetId': 'scratch',
      'tableId': 'results'}
   
  run_query_job(service, project_id, query, print_results,
      destination_table=destination)

if __name__ == "__main__":
    main()
