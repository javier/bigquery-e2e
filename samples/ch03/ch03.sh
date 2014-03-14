# This script contains the list of commands covered in chapter 3.
# Do not try to run this script. Instead, use it to copy and paste
# the commands to save some typing.
exit

# Basic client interaction.
bq
bq --help
bq ls -p
bq ls
bq ls reference
bq --format=csv ls -j
bq ls publicdata:samples

# Service account setup.
cat $HOME/.bigqueryrc

# Replace account-id with whatever label you find convenient.
export SERVICE_ACCOUNT_ID=”account-id”
# You need to be in the directory containing the unpacked chapter download.
# The first argument should be modified to the downloaded API client info.
# The second argument should be updated to point to the private key file.
python make_service_account_rc.py \
    $HOME/Downloads/client_secrets.json \
    $HOME/.bigquery.privatekey.p12 \
    >$HOME/.bigqueryrc.$SERVICE_ACCOUNT_ID

bq --bigqueryrc=$HOME/.bigqueryrc.$SERVICE_ACCOUNT_ID ls

# Run this if you get an error saying SSL support is absent.
easy_install pyOpenSSl

export BIGQUERYRC=$HOME/.bigqueryrc.$SERVICE_ACCOUNT_ID
bq ls –j
bq query '
    SELECT zip FROM [bigquery-e2e:reference.zip_codes]
    WHERE area_codes CONTAINS “425” LIMIT 3'
bq –format=csv ls –j

# Extract the table from BigQuery to Google Cloud Storage.
BUCKET='<Google Cloud Storage bucket>'
bq extract bigquery-e2e:reference.zip_codes gs://${BUCKET}/zip_codes.csv

gsutil ls
gsutil ls gs://${BUCKET}/
gsutil cat -r 0-300 gs://${BUCKET}/zip_codes.csv

# Installing all the book samples.
DOWNLOADS="http://storage.googleapis.com/bigquery-e2e/downloads"
curl ${DOWNLOADS}/bigquery_e2e_samples.zip -O
unzip bigquery_e2e_samples.zip
cd bigquery_e2e_samples
# To use the API libraries that are packaged with the samples.
# Command assumes that you are currently in the samples directory.
export PYTHONPATH=${PYTHONPATH}:$(pwd)/lib

