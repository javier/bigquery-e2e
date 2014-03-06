import json
import os
import sys

if len(sys.argv) != 3:
  print sys.stderr, """Usage:
  python make_service_account_rc.py <client secrets file> <key file>
"""
  sys.exit(1)

secrets = json.load(open(sys.argv[1]))
service_account = secrets['web']['client_email']
account_id, _ = service_account.split('@')
token_file = "%s/.bigquery.%s.token" % (
    os.getenv('HOME', os.getenv('USERPROFILE')), account_id)
project_id, _ = secrets['web']['client_id'].split('.', 1)

print (
    "service_account = %s\n"
    "service_account_credential_file = %s\n"
    "service_account_private_key_file = %s\n"
    "project_id = %s\n") % (
    service_account,
    token_file,
    sys.argv[2],
    project_id)

