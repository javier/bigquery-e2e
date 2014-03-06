#!/usr/bin/python2.7

# Python imports
import io
import json
import time

# Google APIs imports
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseUpload

# BigQuery e2e imports
import auth

## Runs through each BigQuery API request.
def run_tour(http, service, project_id):
  print 'Running BigQuery API tour'

  projects = service.projects()
  datasets = service.datasets()
  tables = service.tables()
  tabledata = service.tabledata()
  jobs = service.jobs()

  # Generate some ids to use with the tour.
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
  for project in projects.list().execute(http)['projects']:
     if (project['id'] == project_id):
       print "Found %s: %s" % (project_id, project['friendlyName'])

  # Now create a dataset
  dataset = {'datasetReference': dataset_ref}
  dataset = datasets.insert(body=dataset, **project_ref).execute(http)

  # Patch the dataset to set a friendly name.
  update = {'friendlyName': 'Tour dataset'}
  dataset = datasets.patch(body=update, **dataset_ref).execute(http)

  # Print out the dataset for posterity
  print "%s" % (dataset,)

  # Find our dataset in the datasets list:
  dataset_list = datasets.list(**project_ref).execute(http)
  for current in dataset_list['datasets']:
    if current['id'] == dataset['id']: print "found %s" % (dataset['id'])

  ### Now onto tables...
  table = {'tableReference': table_ref}
  table = tables.insert(body=table, **dataset_ref).execute(http)

  # Update the table to add a schema:
  table['schema'] = {'fields': [{'name': 'a', 'type': 'string'}]}
  table = tables.update(body=table, **table_ref).execute(http)

  # Patch the table to add a friendly name
  patch = {'friendlyName': "Friendly table"}
  table = tables.patch(body=patch, **table_ref).execute(http)

  # Print table for posterity:
  print table

  # Find our table in the tables list:
  table_list = tables.list(**dataset_ref).execute(http)
  for current in table_list['tables']:
    if current['id'] == table['id']: print "found %s" % (table['id'])

  ## And now for some jobs...
  config = {'load': {'destinationTable': table_ref}}
  load_text = 'first\nsecond\nthird'

  # Remember to always name your jobs!
  job = {'jobReference': job_ref, 'configuration': config}

  media = MediaIoBaseUpload(io.BytesIO(load_text), mimetype='application/octet-stream')
  job = jobs.insert(body=job, media_body=media, **project_ref).execute(http)

  # List our running or pending jobs:
  job_list = jobs.list(
      stateFilter=['pending', 'running'], 
      **project_ref).execute(http)
  print job_list

  while job['status']['state'] <> 'DONE':
    job = jobs.get(**job_ref).execute(http)

  # Now run a query against that table.
  query = 'select count(*) from [%s]' % (table['id'])
  query_request = {'query': query, 'timeoutMs': 0, 'maxResults': 1}
  results = jobs.query(body=query_request, **project_ref).execute(http)
  while not results['jobComplete']:
    get_results_request = results['jobReference'].copy()
    get_results_request['timeoutMs'] = 10000
    get_results_request['maxResults'] = 10
    results = jobs.getQueryResults(
	**get_results_request).execute(http)
  print results    

  # Now let's read the data from our table.
  data = tabledata.list(**table_ref).execute(http)
  table = tables.get(**table_ref).execute(http)
  print 'Table %s\nData:%s' % (data, table)

  # Now we should clean up our toys.
  tables.delete(**table_ref).execute(http)
  datasets.delete(**dataset_ref).execute(http)

  # Now try reading the datast after deleting it:
  try:
    datasets.get(**dataset_ref).execute(http)
    print "That's funny, we should never get here!"
  except HttpError as err:
    print err

  # Done!

def main():
  creds = auth.get_creds()
  http = auth.authorize(creds)
  service = build('bigquery', 'v2')
  project_id = 'bigquery-e2e'
  run_tour(http, service, project_id)

if __name__ == "__main__":
    main()
