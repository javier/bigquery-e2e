import json
import sys

from apiclient.discovery import build
import httplib2
# Sample code authorization support.
import auth

def cell(row, index):
  return row['f'][index]['v']

# Set this to your sample project id.
PROJECT_ID = 0

# Find the appropriate set of tables.
bq = build('bigquery', 'v2',
           http=auth.get_creds().authorize(httplib2.Http()))
resp = bq.jobs().query(
    projectId=PROJECT_ID,
    body={'query':('SELECT table_id FROM ch11.__DATASET__ '
                   'WHERE LEFT(table_id, 2) = "%s_"'
                   % (sys.argv[1]))}
).execute()

# Build the list of tables.
tables = ', '.join(['ch11.' + cell(r, 0) for r in resp['rows']])

# Run the final query.
resp = bq.jobs().query(
    projectId=PROJECT_ID,
    body={'query':('SELECT kind, COUNT(day) FROM %s '
                   'GROUP BY 1' % tables)}
).execute()

print "% 5s | % 5s" % ('kind', 'count')
for r in resp['rows']:
  print "% 5s | % 5d" % (cell(r, 0), int(cell(r, 1)))
