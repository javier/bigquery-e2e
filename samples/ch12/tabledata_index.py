# Listing 12.3: Reading all of the data from a table in parallel.
import json
import os
import threading
import time

from apiclient.discovery import build
from apiclient.errors import HttpError
import httplib2
# Sample code authorization support.
import auth

# Set this to your sample project id.
PROJECT_ID = 'bigquery-e2e'
SOURCE_PROJECT_ID = 'publicdata'
SOURCE_DATASET_ID = 'samples'
SOURCE_TABLE_ID = 'shakespeare'
PARTITION_COUNT = 10

def GetTableInfo():
  http = auth.get_creds().authorize(httplib2.Http())
  bq = build('bigquery', 'v2', http=http)

  # Read the table.
  table = bq.tables().get(projectId=SOURCE_PROJECT_ID,
                          datasetId=SOURCE_DATASET_ID,
                          tableId=SOURCE_TABLE_ID).execute()
  last_modified = int(table['lastModifiedTime'])
  row_count = int(table['numRows'])
  print '%s last modified at %d' % (
    table['id'], last_modified)
  return (last_modified, row_count)

class ReadThread (threading.Thread):
    def __init__(self, start_row, end_row, file_name, snapshot_time):
        threading.Thread.__init__(self)
        self.start_row = start_row
        self.end_row = end_row
        self.file = open(file_name, 'w')
        self.snapshot_time = snapshot_time
        self.thread_id = '[%d-%d)' % (start_row, end_row)
    def run(self):
      http = auth.get_creds().authorize(httplib2.Http())
      bq = build('bigquery', 'v2', http=http)
      print 'Reading %s' % (self.thread_id,)
      current_row = self.start_row
      page_token = None
      # table_id = '%s@%d' % (SOURCE_TABLE_ID, self.snapshot_time)
      table_id = SOURCE_TABLE_ID
      while current_row < end_row:
        read_index = current_row if page_token is None else None
        try:
          data = bq.tabledata().list(
              projectId=SOURCE_PROJECT_ID,
              datasetId=SOURCE_DATASET_ID,
              tableId=table_id,
              startIndex=read_index,
              pageToken=page_token,
              # maxResults=self.end_row - current_row
              maxResults=min(500, self.end_row - current_row)
              ).execute()
        except HttpError, err:
          # If the error is a rate limit or connection error, wait and try again.
          if err.resp.status in [403, 500, 503]:
            print '%s: Retryable error %s, waiting' % (
                self.thread_id, err.resp.status,)
            time.sleep(5)
            continue
          else: raise

        if not 'rows' in data:
          print '%s @%d: Missing rows in: %s' % (
              self.thread_id, current_row, data)
          break
           
        rows = data.get('rows', [])
        page_token = data.get('pageToken')
          
        for row in rows:
          self.file.write(json.dumps(row))
          self.file.write('\n')
          current_row += 1
          if current_row - self.start_row % 10000 == 0: 
            print '%s @ %d writing %d' % (
                self.thread_id, current_row, current_row - self.start_row)
        if page_token is None and current_row < end_row:
           print '%s @%d: Exiting early: only read %d rows ' % (
               self.thread_id, current_row,  
               current_row - self.start_row)
           break
       
      print 'Exiting %s' % (self.thread_id,)
      self.file.close()
         

last_modified_time, row_count = GetTableInfo()
stride = row_count / PARTITION_COUNT
threads = []
for partition in range(PARTITION_COUNT):
  file_name = '%s.%d.json' % (SOURCE_TABLE_ID, partition)
  start_row = stride * partition
  end_row = start_row + stride if partition + 1 < PARTITION_COUNT else row_count
  threads.append(ReadThread(start_row, end_row, file_name, last_modified_time))
  threads[partition].start()

for partition in range(PARTITION_COUNT):
  threads[partition].join()


