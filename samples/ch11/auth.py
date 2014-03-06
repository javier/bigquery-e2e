#!/usr/bin/python2.7
import httplib2
import json
import os
from oauth2client.client import flow_from_clientsecrets
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.file import Storage
from oauth2client.tools import run

BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'

# Service account and keyfile only used for service account auth.
SERVICE_ACCT = ('317752944021@developer.gserviceaccount.com')
# Service account access will only be enabled if this file is present.
KEY_FILE = 'key.p12'

def get_creds():
 if os.path.exists(KEY_FILE):
   return get_service_acct_creds(
    SERVICE_ACCT, KEY_FILE)
 else:
   return get_oauth2_creds()
  
def get_oauth2_creds():
  flow  = flow_from_clientsecrets('client_secrets.json',
                                  scope=BIGQUERY_SCOPE)
  storage = Storage('bigquery_credentials.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = run(flow, storage)
  else:
    # Make sure we have an up-to-date copy of the creds.
    credentials.refresh(httplib2.Http())
  return credentials

def get_service_acct_creds(service_acct, key_file):
  with open (key_file, 'rb') as f:
    key = f.read();
  creds = SignedJwtAssertionCredentials(
    service_acct, 
    key,
    BIGQUERY_SCOPE) 
  return creds
