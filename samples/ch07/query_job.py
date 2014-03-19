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
import pprint
import time

def print_results(schema, rows):
  ''' Prints query results, given a schema. '''
  for row in rows:
    line = []
    for i in xrange(0, len(schema)):
      cell = row['f'][i]
      field = schema[i]
      line.append({field['name']: cell['v']})
    pprint.pprint(line)

class QueryJob:
  def __init__(self, service, project_id):
    self.service = service
    self.project_id = project_id

  def run(self, query, response_handler=print_results, 
          job_id=None, destination_table=None, allow_large_results=False,
          batch_priority=False):
    '''Run a Query Job and print the results.

      query: text of query to run.
      response_handler: function that is used to process results.
      job_id: optional job id to provide to BigQuery.
      destination_table: if present, the destination table to write 
          the query results to.
      batch_priority: whether to run the query at batch priority
    '''

    query_config = {
        'query': query,
        'allowLargeResults': allow_large_results
    }
    if not job_id:
      # If the caller did not specify a job id, generate one
      # based on the current time.
      job_id = 'job_%d' % int(time.time() * 1000)

    if destination_table:
      # If this is run multiple times, truncate the table and
      # replace it with the new results.
      query_config['writeDisposition'] = 'WRITE_TRUNCATE'
      query_config['destinationTable'] = destination_table
      query_config['allowLargeResults'] = allow_large_results

    if batch_priority:
      query_config['priority'] = 'BATCH'

    job_ref = {'projectId': self.project_id}
    if job_id:
      job_ref['jobId'] = job_id

    job = {
        'configuration': {'query': query_config},
        'jobReference': job_ref
    }
        
    print 'Starting query job "%s"' % (job,)
    job = self.service.jobs().insert(projectId=self.project_id,
        body=job).execute()
    # Fetch the job ID from the running job, in case one wasn't
    # already specified above.
    job_ref = job['jobReference']

    # Wait for the job to complete.
    while job['status']['state'] != 'DONE':
      print 'Waiting for job %s to complete: %s' % (
          job_ref, job['status']['state'])
      time.sleep(1.0)
      job = self.service.jobs().get(
          jobId = job_ref['jobId'],
          projectId = job_ref['projectId']).execute()
      
    if 'errorResult' in job['status']:
      print 'Error %s' % (job['status']['errorResult'],)
      return
      
    # Read the results using TableData.list(). Note that we could
    # also read the results using jobs.getQueryResults(), but for the 
    # purposes of this sample, we wanted to show the TableData equivalent.

    destination_table_ref = job['configuration']['query']['destinationTable']
    schema = self.service.tables().get(
        tableId=destination_table_ref['tableId'],
        datasetId=destination_table_ref['datasetId'],
        projectId=destination_table_ref['projectId']).execute()['schema']
     
    page_token = None
    while True:
      response = self.service.tabledata().list(
          pageToken=page_token,
          tableId=destination_table_ref['tableId'],
          datasetId=destination_table_ref['datasetId'],
          projectId=destination_table_ref['projectId']).execute()
      page_token = response.get('pageToken', None)
      fields = schema.get('fields', [])
      rows = response.get('rows', [])
      response_handler(fields, rows)
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
  query_job = QueryJob(service, project_id)
  if len(argv) < 2:
    query = 'SELECT 17'
  else:
    # The entire rest of the command line is the query.
    query = ' '.join(argv[1:])
  destination = {
      'projectId': project_id,
      'datasetId': 'scratch',
      'tableId': 'results'}
  query_job.run(query, destination_table=destination)

if __name__ == "__main__":
    main(sys.argv[1:])
