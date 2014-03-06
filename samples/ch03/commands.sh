# This script contains the list of commands covered in chapter 3.
# Do not try to run this script. Instead, use it to copy and paste
# the commands to save some typing.

# Installing the BigQuery command line client.
pushd /tmp
curl -O https://google-bigquery-tools.googlecode.com/files/bigquery-2.0.14.tar.gz
tar -xzf bigquery-2.0.14.tar.gz
cd bigquery-2.0.14
python setup.py install
cd ..
rm -r -f bigquery-2.0.14 bigquery-2.0.14.tar.gz
popd

# Substitute these commands for the install command above to install
# it and dependencies in an isolated environment.
mkdir $HOME/bigquery
mkdir $HOME/bigquery/lib
mkdir $HOME/bigquery/bin
PYTHONPATH=$HOME/bigquery/lib python setup.py install \
    --install-lib=$HOME/bigquery/lib \
    --install-scripts=$HOME/bigquery/bin
alias bq='PYTHONPATH=$HOME/bigquery/lib $HOME/bigquery/bin/bq'

# Client setup.
bq init

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
# You need to be in directory containing the unpacked chapter download.
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

# Google Cloud Storage setup.
curl -O http://storage.googleapis.com/pub/gsutil.tar.gz
tar -xfz  gsutil.tar.gz -C $HOME
alias gsutil=$HOME/gsutil/gsutil

gsutil config

# Extract the table from BigQuery to Google Cloud Storage.
bq extract bigquery-e2e:reference.zip_codes gs://<bucket>/zip_codes.csv

gsutil ls
gsutil ls gs://<bucket>/
gsutil cat -r 0-300 gs://<bucket>/zip_codes.csv

# Installing all the book samples.
curl –O https://bigquery-e2e.googlecode.com/files/bigquery_e2e_samples.zip
unzip bigquery_e2e_samples.zip
cd bigquery_e2e_samples

