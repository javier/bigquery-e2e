#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Runs a BigQuery query using the Jobs.insert() API

Runs a BigQuery query via the Jobs.insert() API, waits for it 
to complete, fetches the results via the TableData.list()
API,  then prints out the results.
Usage:
  python query_job.py <project_id>
will run the query SELECT 17 in project <project_id>

  python query.py <project_id> <query text>
will run the query <query text> in project <project_id>
'''

import auth
import sys
import time

def print_results(results, schema):
  fields = schema.get('fields', [])
  rows = results.get('rows', [])
  for row in rows:
    for i in xrange(0, len(fields)): 
      cell = row['f'][i]
      field = fields[i]
      print "%s: %s " % (field['name'], cell['v']),
    print ''

def run_query_job(
    service, project_id, query, response_handler=print_results, 
    job_id=None, destination_table=None, allow_large_results=False,
    batch_priority=False):

  query_config = {
      'query': query,
      'allowLargeResults': allow_large_results
  }

  if destination_table:
    # If this is run multiple times, truncate the table and
    # replace it with the new results.
    query_config['writeDisposition'] = 'WRITE_TRUNCATE'
    query_config['destinationTable'] = destination_table
    query_config['allowLargeResults'] = allow_large_results

  if batch_priority:
    query_config['priority'] = 'BATCH'

  job_ref = {'projectId': project_id}
  if job_id:
    job_ref['jobId'] = job_id

  job = {
      'configuration': {'query': query_config},
      'jobReference': job_ref
  }
      
  print 'Starting query job "%s"' % (job,)
  job = service.jobs().insert(projectId=project_id,
      body=job).execute()
  # Fetch the job ID from the running job, in case one wasn't
  # already specified above.
  job_ref = job['jobReference']

  # Wait for the job to complete.
  while job['status']['state'] != 'DONE':
    print 'Waiting for job %s to complete: %s' % (
        job_ref, job['status']['state'])
    time.sleep(1.0)
    job = service.jobs().get(
        jobId = job_ref['jobId'],
        projectId = project_id).execute()
    
  if 'errorResult' in job['status']:
    print 'Error %s' % (job['status']['errorResult'],)
    return
    
  # Read the results using TableData.list(). Note that we could
  # also read the results using jobs.getQueryResults(), but for the 
  # purposes of this sample, we wanted to show the TableData equivalent.

  destination_table_ref = job['configuration']['query']['destinationTable']
  schema = service.tables().get(
      tableId=destination_table_ref['tableId'],
      datasetId=destination_table_ref['datasetId'],
      projectId=destination_table_ref['projectId']).execute()['schema']
  print 'Output schema: %s' % (schema,)
   
  page_token = None
  while True:
    response = service.tabledata().list(
        pageToken=page_token,
        tableId=destination_table_ref['tableId'],
        datasetId=destination_table_ref['datasetId'],
        projectId=destination_table_ref['projectId']).execute()
    page_token = response.get('pageToken', None)
    response_handler(response, schema)
    if page_token is None:
      # The query is done and there are no more results
      # to read.
      break

def main(argv):
  if len(argv) == 0:
    print('Usage: query_job.py <project_id> [query]')
    return
  service = auth.build_bq_client()
  project_id = argv[0]
  if len(argv) < 2:
    query = 'SELECT 17'
  else:
    # The entire rest of the command line is the query.
    query = ' '.join(argv[1:])
  destination = {
      'projectId': project_id,
      'datasetId': 'scratch',
      'tableId': 'results'}
  run_query_job(service, project_id, query, print_results,
      destination_table=destination)

if __name__ == "__main__":
    main(sys.argv[1:])
