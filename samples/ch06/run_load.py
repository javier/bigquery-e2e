'''Common functions used to execute load jobs.'''

import json
import time

def start_and_wait(jobs, project_id, load, media_body=None):
  '''Run a load job with the given specification.
  
    jobs: client for the jobs collection in the service.
    project_id: project ID under which the job will run.
    load: the load job configuration.
    media_body: optional media object, ie file, to upload.
  '''
  start = time.time()
  job_id = 'ch06_%d' % start
  # Create the job.
  result = jobs.insert(
    projectId=project_id,
    body={
      'jobReference': {
        'jobId': job_id
      },
      'configuration': {
        'load': load
      }
    },
    media_body=media_body).execute()
  print json.dumps(result, indent=2)
  # Wait for completion.
  done = False
  while not done:
    time.sleep(5)
    result = jobs.get(projectId=project_id, jobId=job_id).execute()
    print "%s %ds" % (result['status']['state'], time.time() - start)
    done = result['status']['state'] == 'DONE'
  # Print all errors and warnings.
  for err in result['status'].get('errors', []):
    print json.dumps(err, indent=2)
  # Check for failure.
  if 'errorResult' in result['status']:
    print 'FAILED'
    print json.dumps(result['status']['errorResult'], indent=2)
  else:
    print 'SUCCESS'
