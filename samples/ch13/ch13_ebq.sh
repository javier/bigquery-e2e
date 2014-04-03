#!/usr/bin/bash -v
#
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.
#
# Chapter 13 bash commands for using Encrypted BigQuery (ebq).
# To use this script, run
# source ch13_ebq.sh
# This will install ebq and quit. You should already have 
# bq installed, and set the GCS_BUCKET environment variable
# to a Google Cloud Storage bucket that you own. For instance:
# GCS_BUCKET=bucket_name
#
# To run the individual commands,  cut and paste them
# onto the command line.

# Make sure the ch13 dataset exists.
bq mk -d ch13

# Install Encrypted BigQuery.
easy_install encrypted_bigquery

# Verify ebq instllation.
ebq --helpshort

# Exits the script. The remaining comands should be cut and pasted
# if you want to run them.
return

# Download Shakespeare table locally for encryption.
bq extract --destination_format=NEWLINE_DELIMITED_JSON \
    "publicdata:samples.shakespeare" \
    gs://${GCS_BUCKET}/shakespeare.json
gsutil cp gs://${GCS_BUCKET}/shakespeare.json .

# Get table schema.
bq --format=prettyjson show publicdata:samples.shakespeare > table.txt

# Not shown: Manually edit table.txt to contain only the schema fields,
# and add encryption type specifiers "probabilistic, homomorphic, 
# pseudonym, and none" to the table's fields. encrypted_schema.txt
# has the expected result; you can use that instead.

# Now encrypt the local file and load it into an encrypted table.
ebq --master_key_filename="ebq.key" load \
    --source_format=NEWLINE_DELIMITED_JSON \
    ch13.enc_shakes \
    shakespeare.json \
    encrypted_schema.txt

# Show information about the encrypted table.
ebq --master_key_filename=ebq.key show ch13.enc_shakes

# Run a query over the encrypted data.
ebq --master_key_filename=ebq.key query \
     "SELECT corpus, COUNT(word_count) 
      FROM ch13.enc_shakes 
      GROUP BY corpus"

# Try running a group by over a probabilistically encrypted field.
# This should fail:
ebq --master_key_filename=ebq.key query \
    "SELECT word, 
     COUNT(word_count) 
     FROM ch13.enc_shakes 
     GROUP BY word"

ebq --master_key_filename=ebq.key query "
    SELECT SUM(word_count)
    FROM ch13.enc_shakes
    WHERE corpus = 'hamlet'"

