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
import sys

def print_results(response):
  fields = response.get('schema', []).get('fields', [])
  rows = response.get('rows', [])
  for row in rows:
    for i in xrange(0, len(fields)): 
      cell = row['f'][i]
      field = fields[i]
      print "%s: %s " % (field['name'], cell['v']),
    print ''

def run_query(service, project_id, query, response_handler=print_results, 
              timeout=30*1000, max_results=1024):
  query_request = {
      'query': query,
       # Use a timeout of 0, which means we'll always need
       # to get results via getQueryResults().
      'timeoutMs': 0,
      'maxResults': max_results
  }
  print 'Running query "%s"' % query

  # Start the query.
  response = service.jobs().query(
      projectId=project_id,
      body=query_request).execute()
  job_ref = response['jobReference']

  while True:
    print 'Response %s' % (response,)
    page_token = response.get('pageToken', None)
    query_complete = response.get('jobComplete', False)
    if query_complete:
      response_handler(response)
      if page_token is None:
        # The query is done and there are no more results
        # to read.
        break
    response = service.jobs().getQueryResults(
        projectId = project_id,
        jobId = job_ref['jobId'],
        timeoutMs = timeout,
        pageToken = page_token,
        maxResults = max_results).execute()

def main(argv):
  if len(argv) == 0:
    print('Usage: query.py <project_id> [query]')
    return
  service = auth.build_bq_client() 
  project_id = argv[0]
  if len(argv) < 2:
    query = 'SELECT 17'
  else:
    # The entire rest of the command line is the query.
    query = ' '.join(argv[1:])
  run_query_job(service, project_id, query, print_results, timeout=1)

if __name__ == "__main__":
    main(sys.argv[1:])

