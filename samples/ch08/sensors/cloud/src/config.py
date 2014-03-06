import httplib2
from oauth2client.appengine import AppAssertionCredentials
from apiclient import discovery
from google.appengine.api import memcache

# Modify the next line to specify your BigQuery project id.
PROJECT_ID = 'bigquery-e2e'

# openssl pkcs12 -in key.p12 -out key.pem -nodes -nocerts
# openssl rsa -in key.pem -out /tmp/key-rsa.pem
# use key-rsa.pem
# --appidentity_email_address <id>@developer.gserviceaccount.com
# --appidentity_private_key_path /tmp/key-rsa.pem
credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')
bigquery = discovery.build('bigquery', 'v2',
                           http=credentials.authorize(httplib2.Http(memcache)))
#from google.appengine.api import app_identity
#access_token = app_identity.get_access_token(
#    'https://www.googleapis.com/auth/bigquery')
