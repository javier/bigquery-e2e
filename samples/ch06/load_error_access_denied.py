# Sample code authorization support.
import auth
# Functions to help run a load job.
import run_load

def main():
  service = auth.build_bq_client()

  # Load configuration with the destination specified.
  load_config = {
    'destinationTable': {
      'projectId': 'publicdata',
      'datasetId': 'samples',
      'tableId': 'mypersonaltable'
    }
  }
  # Setup the job here.
  # load[property] = value
  load_config['sourceUris'] = [
    'gs://bigquery-e2e/chapters/06/sample.csv',
  ]
  # End of job configuration.

  run_load.start_and_wait(service.jobs(),
                          auth.PROJECT_ID,
                          load_config)

if __name__ == "__main__":
    main()
