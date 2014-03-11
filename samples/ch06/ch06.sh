# This file contains the terminal commands discussed in the Chapter 6.
# It is not intended to be executed but you can source the file:
#   source ch06.sh
# to setup the environment variables needed by the commands.
BASE_URL=https://www.googleapis.com/bigquery/v2
# Edit this line to use your project id.
PROJECT_ID=317752944021
PROJECT_URL=${BASE_URL}/projects/${PROJECT_ID}
DATASET_URL=${PROJECT_URL}/datasets/ch06
# Define a function construct the urls for a specific table.
table_url() {
    echo -n ${DATASET_URL}/tables/${1}
}

if false; then

# Copy the commands from this file into your terminal.
# Edit them, if necessary, before running them.
echo 'This part of the script will be skipped when sourced'

# Running the load job samples.
python load.py
python load_resumable.py
python load_error_access_denied.py
python load_error_bad_data.py

# Create a table for testing streaming inserts.
bq mk -t ch06.streamed ts:timestamp,label:string,count:integer

# Use bq to insert rows into a table.
echo '{"ts":1381186891,"label":"test","count":42}' | \
    bq insert ch06.streamed
# Check that the row made it in.
bq head ch06.streamed

# Insert a row into a non-existent table.
echo '{"rows": [{"json": {"count": "4.2", "ts": 123, "label": "bad"}}]}' |
curl -H "$(python auth.py)" \
  -H 'Content-Type: application/json' --data-binary @- \
  $(table_url foo)/insertAll

# Insert a row (with an error) into a table.
echo '{"rows": [{"json": {"count": "4.2", "ts": 123, "label": "bad"}}]}' |
curl -H "$(python auth.py)" \
  -H 'Content-Type: application/json' --data-binary @- \
  $(table_url streamed)/insertAll

# Start watching a log file.
python stream.py /tmp/log &

# Append a couple of lines to the file.
echo "$(date +%s),http://www.google.com,1396" >>/tmp/log
echo "$(date +%s),http://www.gmail.com,4212" >>/tmp/log

# You can try killing and restarting the process.
# The records will not be duplicated if this is done soon enough.
kill %1
python stream.py /tmp/log &

fi
