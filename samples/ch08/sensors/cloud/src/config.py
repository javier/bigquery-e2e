import httplib2
from oauth2client.appengine import AppAssertionCredentials
from apiclient import discovery
from google.appengine.api import memcache

# Modify the next line to specify your BigQuery project id.
PROJECT_ID = 'bigquery-e2e'

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')
bigquery = discovery.build('bigquery', 'v2',
                           http=credentials.authorize(httplib2.Http(memcache)))
