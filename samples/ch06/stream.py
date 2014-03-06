import json
import os
import sys
import time

from apiclient.discovery import build
import httplib2
# Sample code authorization support.
import auth

# Set this to your sample project id.
PROJECT_ID = 317752944021
DATASET_ID = 'ch06'
TABLE_ID = 'streamed'

infile = open(sys.argv[1], 'a+')
bq = build('bigquery', 'v2',
           http=auth.get_creds().authorize(httplib2.Http()))
tabledata = bq.tabledata()

pos = 0
rows = []
while True:
    line = infile.readline() if len(rows) < 10 else None
    if line and line[-1] == '\n':
        pos = infile.tell()
        ts, label, count = line.split(',')
        rows.append(
            {'insertId': '%s%d' % (sys.argv[1], pos),
             'json': dict(ts=int(ts.strip()),
                          label=label.strip(),
                          count=int(count.strip()))})
    else:
        if rows:
            tabledata.insertAll(
                projectId=PROJECT_ID,
                datasetId=DATASET_ID,
                tableId=TABLE_ID,
                body=dict(rows=rows)).execute()
            del rows[:]
        else:
            time.sleep(0.1)
        infile.seek(pos)
