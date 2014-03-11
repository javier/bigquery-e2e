import sys
import time

# Sample code authorization support.
import auth

def tail_and_insert(infile,
                    tabledata,
                    project_id, 
                    dataset_id,
                    table_id):
  '''Tail a file and stream its lines to a BigQuery table.

    infile: file object to be tailed.
    project_id: project ID of the destination table.
    dataset_id: dataset ID of the destination table.
    table_id: table ID of the destination table.
  '''
  pos = 0
  rows = []
  while True:
    # If the file has additional data available and there are less than
    # 10 buffered rows then fetch the next available line.
    line = infile.readline() if len(rows) < 10 else None
    # If the line is a complete line buffer it.
    if line and line[-1] == '\n':
      # Record the end of the last full line.
      pos = infile.tell()
      ts, label, count = line.split(',')
      rows.append({
          'insertId': '%s%d' % (sys.argv[1], pos),
          'json': {
            'ts': int(ts.strip()),
            'label': label.strip(),
            'count': int(count.strip())
          }
      })
    # 10 buffered rows or no new data so flush buffer by positing it.
    else:
      if rows:
        tabledata.insertAll(
          projectId=project_id,
          datasetId=dataset_id,
          tableId=table_id,
          body={'rows': rows}).execute()
        del rows[:]
      else:
        # No new data so sleep briefly.
        time.sleep(0.1)
      # Re-position the file at the end of the last full record.
      infile.seek(pos)

def main():
  service = auth.build_bq_client()

  with open(sys.argv[1], 'a+') as infile:
    tail_and_insert(infile,
                    service.tabledata(),
                    auth.PROJECT_ID,
                    'ch06',
                    'streamed')

if __name__ == '__main__':
  main()
