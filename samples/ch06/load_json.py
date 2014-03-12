# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

# Sample code authorization support.
import auth
# Functions to help run a load job.
import run_load

def main():
  service = auth.build_bq_client()

  # Load configuration with the destination specified.
  load_config = {
    'destinationTable': {
      'projectId': auth.PROJECT_ID,
      'datasetId': 'ch06',
      # You can update this for each example.
      'tableId': 'example_json'
    }
  }
  # Setup the job here.
  # load[property] = value
  load_config['schema'] = {
    'fields': [
      {'name':'string_f', 'type':'STRING'},
      {'name':'boolean_f', 'type':'BOOLEAN', 'mode':'REPEATED'},
      {'name':'record_f', 'type':'RECORD',
       'fields': [
          {'name':'integer_f', 'type':'INTEGER'},
          {'name':'float_f', 'type':'FLOAT'}
       ]
      },
      {'name':'timestamp_f', 'type':'TIMESTAMP'}
    ]
  }
  # Select the JSON input format.
  load_config['sourceFormat'] = 'NEWLINE_DELIMITED_JSON'
  load_config['sourceUris'] = [
    'gs://bigquery-e2e/chapters/06/sample.json',
  ]
  # End of job configuration.

  run_load.start_and_wait(service.jobs(),
                          auth.PROJECT_ID,
                          load_config)

if __name__ == '__main__':
    main()
