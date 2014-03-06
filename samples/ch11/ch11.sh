# This file contains the terminal commands discussed in Chapter 11.
# It is not intended to be executed. Use it to copy commands into
# your terminal and edit them as appropriate.

# The commands in this file assume that you bigquery command line
# configured with a project that you can modify.
# Some of the commands also require gsutil, the GCS client.

if false; then

echo 'This part of the script will be skipped when sourced'

# Create the dataset for chapter samples.
bq mk ch11

# Sample datastore backups in GCS.
gsutil ls gs://bigquery-e2e/data/backup/datastore/001/*.backup_info

BACKUP_PATH='bigquery-e2e/data/backup/datastore/001'
BACKUP_ID=`
`'ahJzfmJpZ3F1ZXJ5LXNlbnNvcnNyPwsSHF9BRV9EYXRhc3RvcmVBZG1pb'`
`'l9PcGVyYXRpb24YkU4MCxIWX0FFX0JhY2t1cF9JbmZvcm1hdGlvbhgBDA'
bq load \
 --source_format=DATASTORE_BACKUP \
 ch11.devices \
 gs://${BACKUP_PATH}/${BACKUP_ID}.Device.backup_info

bq show ch11.devices

bq query 'SELECT __key__.name, __has_error__, __error__ FROM ch11.devices'

bq query \
'SELECT __key__.name, 
        owner.email, make, model
 FROM ch11.devices'

BACKUP_PATH='bigquery-e2e/data/backup/datastore/002'
BACKUP_ID=`
`'ahJzfmJpZ3F1ZXJ5LXNlbnNvcnNyQAsSHF9BRV9EYXRhc3RvcmVBZG1pb'`
`'l9PcGVyYXRpb24YseoBDAsSFl9BRV9CYWNrdXBfSW5mb3JtYXRpb24YAQw'
bq load \
 --source_format=DATASTORE_BACKUP \
 ch11.devices_multi_type \
 gs://${BACKUP_PATH}/${BACKUP_ID}.Device.backup_info
bq show ch11.devices_multi_type

bq show ch11.devices_multi_type

bq query \
'SELECT 
    storage_gb.float, 
    storage_gb.integer,
    storage_gb.provided
 FROM ch11.devices_multi_type'

bq query \
'SELECT IF(storage_gb.float IS NULL,
           FLOAT(storage_gb.integer),
           storage_gb.float) storage_gb
 FROM ch11.devices_multi_type'

for i in $(seq 10); do
  echo $i
  bq query --append_table \
    --destination_table=ch11.time_lapse \
    "SELECT ${i} index, INTEGER(NOW()/1000) millis"
  sleep 10
done

bq head ch11.time_lapse@1384381611449

# Helpful expression to find time delta in millis.
echo $(( $(date +%s)000 - 1384381617535))

# 60000 = 1 minute ago
bq head ch11.time_lapse@-60000

bq head ch11.time_lapse@-60000-

bq head ch11.time_lapse@1384381608691-1384381619201

bq query \
'SELECT MIN(index), MAX(index) 
 FROM [ch11.time_lapse@1384381608691-1384381619201]'

bq cp ch11.time_lapse@1384381624603 ch11.recovered
bq head ch11.recovered

for d in $(seq 0 6); do
  day=$(date -d "$d days ago" +%Y%m%d)
  for kind in a b; do
    echo $kind $day
    bq query --destination_table=ch11.${kind}_${day} \
      "SELECT \"${kind}\" kind, \"${day}\" day"
  done
done

bq query \
"SELECT table_id FROM ch11.__DATASET__ 
 WHERE REGEXP_MATCH(table_id, r'^(a|b)_')"

(input="a"
  bq query \
  "SELECT kind, count(day) [count]
   FROM (TABLE_QUERY(ch11, 'LEFT(table_id, 2) = \"${input}_\"'))
   GROUP BY 1")

bq query \
"SELECT kind, MIN(day), MAX(day)
 FROM (TABLE_DATE_RANGE(
           ch11.a_,
           DATE_ADD(CURRENT_TIMESTAMP(), -3, 'DAY'),
           CURRENT_TIMESTAMP()))
 GROUP BY 1"

bq query \
"SELECT kind, MIN(day), MAX(day) 
 FROM (TABLE_DATE_RANGE(
           ch11.b_,
           TIMESTAMP('2013-11-09'),
           TIMESTAMP('2013-11-11'))) GROUP BY 1"

fi
