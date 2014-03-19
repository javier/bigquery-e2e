#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Runs a Knight's Tour of the Google BigQuery API.

This module runs through each of the BigQuery API calls exactly
once as a way to introduce uers to the API.
Running:
  python tour.py <project_id>
where <project_id> is the project ID to use to run the the
BigQuery operations in.
'''
# Python imports
import io
import json
import sys
import time

# Google APIs imports
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseUpload

# BigQuery e2e imports
import auth

## Runs through each BigQuery API request.
def run_tour(service, project_id):
  print 'Running BigQuery API tour'

  projects = service.projects()
  datasets = service.datasets()
  tables = service.tables()
  tabledata = service.tabledata()
  jobs = service.jobs()

  # Generate some IDs to use with the tour.
  tour = 'tour_%d' % (time.time())
  dataset_id = 'dataset_' + tour
  table_id = 'table_' + tour
  job_id = 'job_' + tour

  project_ref = {'projectId': project_id}
  dataset_ref = {'datasetId': dataset_id,
                 'projectId': project_id}
  table_ref = {'tableId': table_id,
               'datasetId': dataset_id,
               'projectId': project_id}
  job_ref = {'jobId': job_id,
             'projectId': project_id}

  # First, find the project and print out the friendly name.
  for project in projects.list().execute()['projects']:
    if (project['id'] == project_id):
      print 'Found %s: %s' % (project_id, project['friendlyName'])

  # Now create a dataset
  dataset = {'datasetReference': dataset_ref}
  dataset = datasets.insert(body=dataset, **project_ref).execute()

  # Patch the dataset to set a friendly name.
  update = {'friendlyName': 'Tour dataset'}
  dataset = datasets.patch(body=update, **dataset_ref).execute()

  # Print out the dataset for posterity
  print '%s' % (dataset,)

  # Find our dataset in the datasets list:
  dataset_list = datasets.list(**project_ref).execute()
  for current in dataset_list['datasets']:
    if current['id'] == dataset['id']:
      print 'found %s' % (dataset['id'])

  ### Now onto tables...
  table = {'tableReference': table_ref}
  table = tables.insert(body=table, **dataset_ref).execute()

  # Update the table to add a schema:
  table['schema'] = {'fields': [{'name': 'a', 'type': 'string'}]}
  table = tables.update(body=table, **table_ref).execute()

  # Patch the table to add a friendly name
  patch = {'friendlyName': 'Friendly table'}
  table = tables.patch(body=patch, **table_ref).execute()

  # Print table for posterity:
  print table

  # Find our table in the tables list:
  table_list = tables.list(**dataset_ref).execute()
  for current in table_list['tables']:
    if current['id'] == table['id']:
      print 'found %s' % (table['id'])

  ## And now for some jobs...
  config = {'load': {'destinationTable': table_ref}}
  load_text = 'first\nsecond\nthird'

  # Remember to always name your jobs!
  job = {'jobReference': job_ref, 'configuration': config}

  media = MediaIoBaseUpload(io.BytesIO(load_text),
                            mimetype='application/octet-stream')
  job = jobs.insert(body=job, 
                    media_body=media, 
                    **project_ref).execute()

  # List our running or pending jobs:
  job_list = jobs.list(
      stateFilter=['pending', 'running'], 
      **project_ref).execute()
  print job_list

  while job['status']['state'] <> 'DONE':
    job = jobs.get(**job_ref).execute()

  # Now run a query against that table.
  query = 'select count(*) from [%s]' % (table['id'])
  query_request = {'query': query, 'timeoutMs': 0, 'maxResults': 1}
  results = jobs.query(body=query_request, **project_ref).execute()
  while not results['jobComplete']:
    get_results_request = results['jobReference'].copy()
    get_results_request['timeoutMs'] = 10000
    get_results_request['maxResults'] = 10
    results = jobs.getQueryResults(
        **get_results_request).execute()
  print results    

  # Now let's read the data from our table.
  data = tabledata.list(**table_ref).execute()
  table = tables.get(**table_ref).execute()
  print 'Table %s\nData:%s' % (data, table)

  # Now we should clean up our toys.
  tables.delete(**table_ref).execute()
  datasets.delete(**dataset_ref).execute()

  # Now try reading the dataset after deleting it:
  try:
    datasets.get(**dataset_ref).execute()
    print "That's funny, we should never get here!"
  except HttpError as err:
    print 'Expected error:\n%s' % (err,)

  # Done!

def main(argv):
  service = auth.build_bq_client() 
  project_id = 'bigquery-e2e' if len(argv) == 0 else argv[0]
  run_tour(service, project_id)

if __name__ == '__main__':
    main(sys.argv[1:])
