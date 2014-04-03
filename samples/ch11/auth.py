#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Handles credentials and authorization.

This module is used by the sample scripts to handle credentials and
generating authorized clients. The module can also be run directly
to print out the HTTP authorization header for use in curl commands.
Running:
  python auth.py
will print the header to stdout. Note that the first time this module
is run (either directly or via a sample script) it will trigger the
OAuth authorization process.
'''
import httplib2
import json
import os
import sys

HAS_CRYPTO = False

from apiclient import discovery
from oauth2client.client import flow_from_clientsecrets
try: 
  # Some systems may not have OpenSSL installed so can't use
  # SignedJwtAssertionCredentials.
  from oauth2client.client import SignedJwtAssertionCredentials
  HAS_CRYPTO = true
except ImportError:
  pass

from oauth2client import tools
from oauth2client.file import Storage

BIGQUERY_SCOPE = 'https://www.googleapis.com/auth/bigquery'

# Service account and keyfile only used for service account auth.
SERVICE_ACCT = ('<service account id>@developer.gserviceaccount.com')
# Set this to the full path to your service account private key file.
KEY_FILE = 'key.p12'

def get_creds():
  '''Get credentials for use in API requests.

  Generates service account credentials if the key file is present,
  and regular user credentials if the file is not found.
  ''' 
  if os.path.exists(KEY_FILE):
    return get_service_acct_creds(SERVICE_ACCT, KEY_FILE)
  else:
    return get_oauth2_creds()
  
def get_oauth2_creds():
  '''Generates user credentials.
  
  Will prompt the user to authorize the client when run the first time.
  Saves the credentials in ~/bigquery_credentials.dat.
  '''
  flow  = flow_from_clientsecrets('client_secrets.json',
                                  scope=BIGQUERY_SCOPE)
  storage = Storage(os.path.expanduser('~/bigquery_credentials.dat'))
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    flags = tools.argparser.parse_args([])
    credentials = tools.run_flow(flow, storage, flags)
  else:
    # Make sure we have an up-to-date copy of the creds.
    credentials.refresh(httplib2.Http())
  return credentials

def get_service_acct_creds(service_acct, key_file):
  '''Generate service account credentials using the given key file.
  
  service_acct: service account ID.
  key_file: path to file containing private key.
  '''
  if not HAS_CRYPTO:
    raise Exception("Unable to use cryptographic functions "
                    + "Try installing OpenSSL")
  with open (key_file, 'rb') as f:
    key = f.read();
  creds = SignedJwtAssertionCredentials(
    service_acct, 
    key,
    BIGQUERY_SCOPE)
  return creds

def authorize(credentials):
  '''Construct a HTTP client that uses the supplied credentials.'''
  return credentials.authorize(httplib2.Http())

def print_creds(credentials):
  '''Prints the authorization header to use in HTTP requests.'''
  cred_dict = json.loads(credentials.to_json())
  if 'access_token' in cred_dict:
    print 'Authorization: Bearer %s' % (cred_dict['access_token'],)
  else:
    print 'creds: %s' % (cred_dict,)

def build_bq_client():
  '''Constructs a bigquery client object.'''
  return discovery.build('bigquery', 'v2',
                         http=get_creds().authorize(httplib2.Http()))

def main():
  print_creds(get_creds())


if __name__ == "__main__":
    main()
