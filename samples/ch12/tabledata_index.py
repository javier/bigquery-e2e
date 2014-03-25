#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Lists table data in parallel threads and writes to local files.

This module reads a BigQuery table using table row offset indexing,
in a number of parallel threads. It writes the results to the local
filesystem.
Running:
  python tabledata_indexed.py <project_id> \
      <dataset_id> <table_id> \
      <destination_directory> \
      <parallel_read_count>
will read the table project_id:dataset_id.table_id in
<paralel_read_count> threads and save the results to
<destination_directory>.
'''

import os
import sys
import time

# Imports from files in this directory:
from table_reader import TableReader
from table_reader import TableReadThread

def parallel_indexed_read(partition_count,
    project_id, dataset_id, table_id, output_dir):
  '''Divides up a table and reads the pieces in parllel by index.'''
  table_reader = TableReader(project_id, dataset_id, table_id)
  _, row_count = table_reader.get_table_info()
  snapshot_time = int(time.time() * 1000)
  stride = row_count / partition_count
  threads = []
  for index in range(partition_count):
    file_name = '%s.%d' % (os.path.join(output_dir, table_id), index)
    start_index = stride * index
    thread_reader = TableReader(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id='%s@%d' % (table_id, snapshot_time),
        start_index=start_index,
        read_count=stride)
    read_thread = TableReadThread(
        thread_reader,
        file_name,
        thread_id='[%d-%d)' % (start_index, start_index + stride))
    threads.append(read_thread)
    threads[index].start()

  for index in range(partition_count):
    threads[index].join()

def main(argv):
  if len(argv) == 0:
     argv = ['publicdata',
             'samples',
             'shakespeare',
             '/tmp/bigquery',
              4]
  if len(argv) < 4:
    # Wrong number of args, print the usage and quit.
    arg_names = [sys.argv[0],
                 '<project_id>',
                 '<dataset_id>',
                 '<table_id>',
                 '<output_directory>',
                 '<partition_count>']
    print 'Usage: %s' % (' '.join(arg_names))
    print 'Got: %s' % (argv,)
    return

  parallel_indexed_read(
      project_id=argv[0],
      dataset_id=argv[1],
      table_id=argv[2],
      output_dir=argv[3],
      partition_count=argv[4])

if __name__ == "__main__":
    main(sys.argv[1:])

