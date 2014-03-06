#!/usr/bin/python2.7
import httplib2
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build

BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'

# Service account and keyfile only used for service account auth.
SERVICE_ACCT = ('317752944021@developer.gserviceaccount.com')
# Service account access will only be enabled if this file is present.
KEY_FILE = 'dev_auth/key.pem'
def get_bigquery():
  with open (KEY_FILE, 'rb') as f:
    key = f.read();
  creds = SignedJwtAssertionCredentials(
    SERVICE_ACCT, 
    key,
    BIGQUERY_SCOPE) 
  return build('bigquery', 'v2', http=creds.authorize(httplib2.Http()))
