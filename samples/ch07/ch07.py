#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Runs Python commands used in Chapter 7'''

import auth
import pprint
project_id = 'bigquery-e2e'
service = auth.build_bq_client()
response = service.jobs().query(
    projectId=project_id,
    body={'query': 'SELECT 17'}).execute()
pprint.pprint(response)

import time
job_id = 'job_%d' % int(time.time() * 1000)
response = service.jobs().insert(
    projectId = project_id,
    body={'configuration': {'query': {'query': 'SELECT 17'}},
          'jobReference': {'jobId': job_id, 'projectId': project_id}}
    ).execute()
pprint.pprint(response)

response = service.jobs().get(projectId=project_id, jobId=job_id).execute()

response = service.jobs().get(**response['jobReference']).execute()

pprint.pprint(response)

table_ref = response['configuration']['query']['destinationTable']
results = service.tabledata().list(**table_ref).execute()
pprint.pprint(results)

schema = service.tables().get(**table_ref).execute()['schema']
pprint.pprint(schema)

response = service.jobs().query(
    projectId=project_id,
    body={'query': 'SELECT 17', 'timeoutMs': 1000000}).execute()
pprint.pprint(response)

response = service.jobs().query(
    projectId=project_id,
    body={'query': 'SELECT 42'}).execute()
job = service.jobs().get(**response['jobReference']).execute()
destination_table=job['configuration']['query']['destinationTable']
pprint.pprint(destination_table)

dataset = service.datasets().get(
    projectId=destination_table['projectId'],
    datasetId=destination_table['datasetId']).execute()
pprint.pprint(dataset)

table = service.tables().get(
    projectId=destination_table['projectId'],
    datasetId=destination_table['datasetId'],
    tableId=destination_table['tableId']).execute()
pprint.pprint(table)

query = 'SELECT COUNT(word), %f FROM [%s]' % (
    time.time(), 'publicdata:samples.shakespeare')
response1 = service.jobs().query(
    projectId=project_id,
    body={'query': query}).execute()
response2 = service.jobs().query(
    projectId=project_id,
    body={'query': query}).execute()
pprint.pprint(response1)
pprint.pprint(response2)

query = """
  SELECT state, COUNT(*) AS cnt 
  FROM [bigquery-e2e:reference.zip_codes] 
  WHERE population > 0 AND decommissioned = false 
  GROUP BY state, ORDER BY cnt DESC
"""
service.jobs().query(
    projectId=project_id,
    body={'query': query, 'useQueryCache': False}
    ).execute()['totalBytesProcessed']

service.tables().get(
    projectId=project_id,
    datasetId='reference',
    tableId='zip_codes').execute()['numRows']

cost_query = """
  SELECT state_len + pop_len + decommissioned_len FROM (
    SELECT SUM(LENGTH(state) + 2) AS state_len, 
      8 * COUNT(population) AS pop_len,
      COUNT(decommissioned) AS decommissioned_len
      FROM [bigquery-e2e:reference.zip_codes])
  """
service.jobs().query(
    projectId=project_id,
    body={'query': cost_query}
    ).execute()['rows'][0]['f'][0]['v']

service.jobs().query(
    projectId=project_id,
    body={'query': query, 'dryRun': True}
    ).execute()['totalBytesProcessed']

