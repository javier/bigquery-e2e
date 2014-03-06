# BigQuery End-To-End chapter 13.
# Using Encrypted BigQuery.

# Install Encrypted BigQuery.
easy_install encrypted_bigquery

# Verify ebq instllation.
ebq --helpshort

# Download Shakespeare table locally for encryption.
bq extract --destination_format=NEWLINE_DELIMITED_JSON \
    "publicdata:samples.shakespeare" \
    gs://bigquery-e2e/shakespeare.json
gsutil cp gs://bigquery-e2e/shakespeare.json .

# Get table schema.
bq --format=prettyjson show publicdata:samples.shakespeare > schema.txt

# Not shown: Manually edit schema.txt to contain only the schema fields,
# and add encryption type specifiers "probabilistic, homomorphic, 
# pseudonym, and none" to the table's fields.

# Now encrypt the local file and load it into an encrypted table.
ebq --master_key_filename="key.key" load \
    --source_format=NEWLINE_DELIMITED_JSON \
    scratch.enc_shakes \
    shakespeare.json \
    schema.txt

# Show information about the encrypted table.
ebq --master_key_filename=key.key show scratch.enc_shakes

# Run a query over the encrypted data.
ebq --master_key_filename=key.key query \
     "SELECT corpus, COUNT(word_count) 
      FROM scratch.enc_shakes 
      GROUP BY corpus 
      ORDER BY corpus ASC"

# Try running a group by over a probabilistically encrypted field.
# This should fail:
ebq --master_key_filename=key.key query \
    "SELECT word, 
     COUNT(word_count) 
     FROM scratch.enc_shakes 
     GROUP BY word"

