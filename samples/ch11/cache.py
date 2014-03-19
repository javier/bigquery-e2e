# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Demonstrates how to implement explicit caching.

  python cache.py <refresh|read>
'''

import json
import sys

# Sample code authorization support.
import auth

CACHE_DATASET = 'ch11'
TOP_APPS_ID = 'top_apps'

def cache_query(jobs, query, cache_id):
  # Must use Jobs.insert() because Jobs.query() does not
  # support a named destination.
  resp = jobs.insert(
    projectId=auth.PROJECT_ID,
    body={
      'configuration': {
        'query': {
          'query': query,
          'destinationTable': {
            'projectId': auth.PROJECT_ID,
            'datasetId': CACHE_DATASET,
            'tableId': cache_id
          },
          'writeDisposition': 'WRITE_TRUNCATE'
        }
      }
    }).execute()
  if 'jobReference' in resp:
    job_id = resp['jobReference']['jobId']
    while not resp.get('jobComplete', False):
      resp = jobs.getQueryResults(
        projectId=auth.PROJECT_ID,
        jobId=job_id,
        # Do not need the data.
        maxResults=0).execute()
  else:
    raise SystemError('Query failed: %s' % json.dumps(resp))
  return resp

def read_cache(tabledata, cache_id):
  rows = []
  resp = {'pageToken': None}
  while 'pageToken' in resp:
    resp = tabledata.list(
      projectId=auth.PROJECT_ID,
      datasetId=CACHE_DATASET,
      tableId=cache_id,
      pageToken=resp['pageToken'],
      maxResults=10000).execute()
    rows.extend([[cell.get('v') for cell in row.get('f')]
                 for row in resp.get('rows', [])])
  return rows

def update_top_apps(jobs):
  return cache_query(
    jobs,
    '''
SELECT 
  running.name AppName,
  AVG(running.memory.total) MemUsage,
  COUNT(running.name) Running'''
# FROM (TABLE_DATE_RANGE(logs.device_,
#                       DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'),
#                       CURRENT_TIMESTAMP()))
# Daily tables are protected so we substitute a sample table.
'''
FROM [bigquery-e2e:ch11.sample_device_log]'''
# Drop the where clause since the sample table is static.
# WHERE
#   (TIMESTAMP_TO_SEC(CURRENT_TIMESTAMP()) -
#    TIMESTAMP_TO_SEC(ts)) < 60 * 60 * 10000
'''
GROUP BY 1
ORDER BY 3 DESC
LIMIT 100''', TOP_APPS_ID)

def format_rows(rows):
  return '\n'.join([','.join(row) for row in rows])

def main(command):
  bq = auth.build_bq_client()
  if command == 'refresh':
    print json.dumps(update_top_apps(bq.jobs()), indent=2)
  elif command == 'read':
    print format_rows(
      read_cache(bq.tabledata(), TOP_APPS_ID))
  else:
    print 'Unknown command: %s' % command

if __name__ == '__main__':
  main(sys.argv[1])
