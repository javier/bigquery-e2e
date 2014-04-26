#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Runs a BigQuery query using the Jobs.query() API

Runs a BigQuery query via the Jobs.query() API, waits for it 
to complete, fetches the results via the Jobs.getQueryResults()
API,  then prints out the results.
Usage:
  python query.py <project_id>
will run a the query SELECT 17' in project <project_id>

  python query.py <project_id> <query text>
will run the query <query text> in project <project_id>
'''

import auth
import pprint
import sys

def print_results(schema, rows):
  ''' Prints query results, given a schema. '''
  for row in rows:
    line = []
    for i in xrange(0, len(schema)):
      cell = row['f'][i]
      field = schema[i]
      line.append({field['name']: cell['v']})
    pprint.pprint(line)

class QueryRpc:
  def __init__(self, service, project_id):
    self.service = service
    self.project_id = project_id

  def run(self, query, response_handler=print_results, 
          timeout_ms=30*1000, max_results=1024):
    '''Run a query RPC and print the results.

      query: text of query to run.
      response_handler: function that is used to process results.
      timeout_ms: timeout of each RPC call.
      max_results: maximum number of results to process.
    '''
    query_request = {
        'query': query,
         # Use a timeout of 0, which means we'll always need
         # to get results via getQueryResults().
        'timeoutMs': 0,
        'maxResults': max_results
    }

    # Start the query.
    response = self.service.jobs().query(
        projectId=self.project_id,
        body=query_request).execute()
    job_ref = response['jobReference']

    while True:
      page_token = response.get('pageToken', None)
      query_complete = response.get('jobComplete', False)
      if query_complete:
        fields = response.get('schema', {}).get('fields', [])
        rows = response.get('rows', [])
        response_handler(fields, rows)
        if page_token is None:
          # The query is done and there are no more results
          # to read.
          break
      response = self.service.jobs().getQueryResults(
          projectId = self.project_id,
          jobId = job_ref['jobId'],
          timeoutMs = timeout_ms,
          pageToken = page_token,
          maxResults = max_results).execute()

def main(argv):
  if len(argv) == 0:
    print 'Usage: query.py <project_id> [query]'
    return
  service = auth.build_bq_client() 
  project_id = argv[0]
  query = QueryRpc(service, project_id)
  if len(argv) < 2:
    query_text = 'SELECT 17'
  else:
    # The entire rest of the command line is the query.
    query_text = ' '.join(argv[1:])

  query.run(query_text, timeout_ms=1)

if __name__ == "__main__":
    main(sys.argv[1:])

