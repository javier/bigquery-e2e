#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Runs Python commands used in Chapter 12'''

import auth
gcs_bucket = 'bigquery-e2e'
project_id = 'bigquery-e2e'

# Using GcsReader
from gcs_reader import GcsReader
GcsReader(gcs_bucket=gcs_bucket,
          download_dir='/tmp/bigquery').read('shakespeare.json')

# Extracting the publicdata:samples.shakespeare table and reading.
from job_runner import JobRunner
import extract_and_read
extract_and_read.run_extract_job(
    JobRunner(project_id=project_id),
    GcsReader(gcs_bucket=gcs_bucket,
              download_dir='/tmp/bigquery'),
    source_project_id='publicdata',
    source_dataset_id='samples',
    source_table_id='shakespeare')

# Partitioned extract and parallel read.
from extract_and_partitioned_read import run_partitioned_extract_job
run_partitioned_extract_job(
    JobRunner(project_id=project_id),
    [GcsReader(gcs_bucket=gcs_bucket,
               download_dir='/tmp/bigquery') for x in range(3)],
    source_project_id='publicdata',
    source_dataset_id='samples',
    source_table_id='shakespeare')

# Reading a table via TableData.list().
from table_reader import TableReader
from table_reader import TableReadThread
output_file_name = '/tmp/bigquery/shakespeare'
table_reader = TableReader(project_id='publicdata',
    dataset_id='samples',
    table_id='shakespeare')
thread = TableReadThread(table_reader, output_file_name)
thread.start()
thread.join()

# Reading a table in parallel by index.
import tabledata_index
tabledata_index.parallel_indexed_read(
    3, 'publicdata', 'samples', 'shakespeare',
    '/tmp/bigquery')


