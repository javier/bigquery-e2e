# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Demonstrates using snapshots for retaining backups.

  python backup.py YYYYMMDD
'''

import datetime
import json
import sys
import time

# Sample code authorization support.
import auth

CACHE_DATASET = 'ch11'
EXPIRATION_MS = 30 * 24 * 60 * 60 * 1000

def wait(jobs, job_ref):
  '''Helper function to block for completion.'''
  start = time.time()
  done = False
  while not done:
    time.sleep(10)
    result = jobs.get(**job_ref).execute()
    print "%s %ds" % (result['status']['state'], time.time() - start)
    done = result['status']['state'] == 'DONE'
  if 'errorResult' in result['status']:
    raise SystemError(json.dumps(
        result['status']['errorResult'], indent=2))

def copy_table(jobs, src, dst):
  '''Insert and wait for a copy job with src and dst.'''
  resp = jobs.insert(
    projectId=auth.PROJECT_ID,
    body={
      'configuration': {
        'copy': {
          'sourceTable': src,
          'destinationTable': dst,
          'writeDisposition': 'WRITE_TRUNCATE'
        }
      }
    }).execute()
  print json.dumps(resp, indent=2)
  wait(jobs, resp['jobReference'])

def make_table_ref(table_id):
  return {
    'projectId': auth.PROJECT_ID,
    'datasetId': 'ch11',
    'tableId': table_id
  }

def load_device_data(jobs, dst):
  # This method simulates loading data from datastore by
  # simply copying a sample table to a new location.
  copy_table(
    jobs,
    src=make_table_ref('devices'),
    dst=dst)

def load_and_backup(bq, date):
  # Get the latest data.
  daily = make_table_ref('devices_' +
                         date.strftime('%Y%m%d'))
  load_device_data(bq.jobs(), daily)

  # Make the snapshot representing the latest.
  current = daily.copy()
  current['tableId'] = 'devices_current'
  copy_table(bq.jobs(), daily, current)

  quarters = {
    '0331': 1,
    '0630': 2,
    '0930': 3,
    '1231': 4
  }
  quarter = quarters.get(date.strftime('%m%d'), None)
  if quarter:
    quarterly = daily.copy()
    quarterly['tableId'] = (
      'devices_%dq%d' % (date.year, quarter))
    copy_table(bq.jobs(), daily, quarterly)

  # Finally set the daily version to expire.
  bq.tables().patch(
    body={
      'expirationTime': long(time.time() * 1000 +
                             EXPIRATION_MS)
    },
    **daily).execute()

def main(date):
  try:
    date = datetime.date(int(date[0:4]),
                         int(date[4:6]),
                         int(date[6:8]))
  except Exception, e:
    print 'Invalid date: %s' % date
  bq = auth.build_bq_client()
  load_and_backup(bq, date)

if __name__ == '__main__':
  main(sys.argv[1])
