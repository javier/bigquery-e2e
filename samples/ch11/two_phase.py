# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Two phase query involving the metadata table.

  python two_phase.py <prefix>
'''

import json
import sys

# Sample code authorization support.
import auth

def cell(row, index):
  return row['f'][index]['v']

# Find the appropriate set of tables.
def get_tables(jobs, prefix):
  resp = jobs.query(
    projectId=auth.PROJECT_ID,
    body={'query':('SELECT table_id FROM ch11.__DATASET__ '
                   'WHERE LEFT(table_id, 2) = "%s_"'
                   % (prefix))}
  ).execute()

  # Build the list of tables.
  return ', '.join(['ch11.' + cell(r, 0)
                    for r in resp.get('rows', [])])

# Run the final query.
def get_data(jobs, tables):
  resp = jobs.query(
    projectId=auth.PROJECT_ID,
    body={'query':('SELECT kind, COUNT(day) FROM %s '
                   'GROUP BY 1' % tables)}
  ).execute()
  return resp['rows']

def main(prefix):
  jobs = auth.build_bq_client().jobs()
  tables = get_tables(jobs, prefix)
  if not tables:
    print 'No tables matched prefix %s' % sys.argv[1]
    return
  data = get_data(jobs, tables)

  print "% 5s | % 5s" % ('kind', 'count')
  for r in data:
    print "% 5s | % 5d" % (cell(r, 0), int(cell(r, 1)))

if __name__ == '__main__':
  main(sys.argv[1])
