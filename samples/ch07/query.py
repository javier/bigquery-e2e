#!/usr/bin/python2.7

import auth

def run_query(service, project_id, query, response_handler, 
              timeout=30*1000, max_results=1024):
  query_request = {
      'query': query,
      'timeoutMs': timeout,
      'maxResults': max_results}
  print 'Running query "%s"' % (query,)
  response = service.jobs().query(projectId=project_id,
      body=query_request).execute()
  job_ref = response['jobReference']

  get_results_request = {
      'projectId': project_id,
      'jobId': job_ref['jobId'],
      'timeoutMs': timeout,
      'maxResults': max_results}
    
  while True:
    print 'Response %s' % (response,)
    page_token = response.get('pageToken', None)
    query_complete = response['jobComplete']
    if query_complete:
      response_handler(response)
      if page_token is None:
        # Our work is done, query is done and there are no more
        # results to read.
        break;
    # Set the page token so that we know where to start reading from.
    get_results_request['pageToken'] = page_token
    # Apply a python trick here to turn the get_results_request dict
    # into method arguments.
    response = service.jobs().getQueryResults(
        **get_results_request).execute()

def print_results(results):
  fields = results['schema']['fields']
  rows = results['rows']
  for row in rows:
    for i in xrange(0, len(fields)): 
      cell = row['f'][i]
      field = fields[i]
      print "%s: %s " % (field['name'], cell['v']),
    print ''

def main():
  service = auth.build_bq_client() 
  project_id = 'bigquery-e2e'
  query = 'select * from temp.nested'
  run_query(service, project_id, query, print_results, timeout=1)

if __name__ == "__main__":
    main()
