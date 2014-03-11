# Import class that handles file uploads.
from apiclient.http import MediaFileUpload

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
      'tableId': 'example_resumable'
    }
  }
  # Setup the job here.
  # load[property] = value
  load_config['schema'] = {
    'fields': [
      {'name':'string_f', 'type':'STRING'},
      {'name':'boolean_f', 'type':'BOOLEAN'},
      {'name':'integer_f', 'type':'INTEGER'},
      {'name':'float_f', 'type':'FLOAT'},
      {'name':'timestamp_f', 'type':'TIMESTAMP'}
    ]
  }

  upload = MediaFileUpload('sample.csv',
                           mimetype='application/octet-stream',
                           # This enables resumable uploads.
                           resumable=True)
  # End of job configuration.

  run_load.start_and_wait(service.jobs(),
                          auth.PROJECT_ID,
                          load_config,
                          media_body=upload)

if __name__ == '__main__':
    main()
