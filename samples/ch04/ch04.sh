#!/usr/bin/bash -v
#
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.
#
# Chapter 4 bash commands. The responses are in ch04.out.
# This script is not intended to be run directly; it just has 
# example command lines showing usage of the BigQuery API via
# the bq tool.

### Quits the bash script
return

### Creates a scratch dataset. This is not in the text, but 
### may be useful if you try this out yourself.
bq mk -d scratch

#### Inserting the same job id twice:
JOB_ID=job_$(date +"%s")
bq --job_id=${JOB_ID} query --max_rows=0 "SELECT 17"
bq --job_id=${JOB_ID} query --max_rows=0 "SELECT 17"

#### Inserting the same job with network error:
JOB_ID=job_$(date +"%s")
bq --job_id=${JOB_ID} query --max_rows=0 "SELECT 42"
# ...fix the network connection...
bq show -j ${JOB_ID}

### Using dry run to test whether a query will work.
bq query --dry_run --format=json "bad query"

### Replacing an existing table.
echo a,b,c > temp.csv
bq load --replace scratch.table1 temp.csv "f1,f2,f3"

#### Using bq.py to inspect job state
# Start a load job to load that csv file with a mismatched schema:
JOB_ID=job_$(date +"%s")
bq --nosync –job_id=${JOB_ID} \
    load scratch.table1 temp.csv "f1,f2"

# Read the job status:
bq show -j ${JOB_ID}
bq show -j ${JOB_ID}
bq show -j ${JOB_ID}

#### Load with an error in errors but no errorResult
JOB_ID=job_$(date +"%s")
bq --job_id=${JOB_ID} \
    load --max_bad_records=1 \
    scratch.table1 temp.csv "f1,f2"

# Wwarnings don’t show up in ‘bq show’ by default, so we use the 
# prettyjson formatter:
bq show -j --format=prettyjson ${JOB_ID}

#### Checking how many bytes were loaded via bq
JOB_ID=job_$(date +"%s")
echo 1,1.0,foo > temp.csv
bq --job_id=${JOB_ID} --format=prettyjson \
    load scratch.table2 temp.csv "f1:integer,f2:float,f3:string"
# Run bq show to display the stats from the job that was run.
bq --format=json show -j ${JOB_ID} | grep outputBytes

#### Finding the number of bytes that would be processed by a query
#### wihtout running it
bq query --dry_run --format=prettyjson \
    "select title from publicdata:samples.wikipedia" \
     | grep totalBytesProcessed

