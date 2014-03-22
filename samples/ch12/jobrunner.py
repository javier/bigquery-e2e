#!/usr/bin/python2.7
# All rights to this package are hereby disclaimed and its contents
# released into the public domain by the authors.

'''Helper class to run BigQuery jobs

This module contains code to start a BigQuery job and wait for
its completion.
Usage:
  job_runner = JobRunner('<project_id>')
  job_runner.start_job(<job_config_dict>)
  job_runner.wait_for_complete()
will start a job in project <project_id> using the job configuration
specified in <job_config_dict>.
'''

import json
import threading
import time

from apiclient.errors import HttpError
# Sample code authorization support.
import auth

class JobRunner:
  def __init__(self, project_id, job_id=None):
    # Only one thread can call the bq_service at once.
    self.lock = threading.Lock()
    self.bq_service = auth.build_bq_client()
    self.project_id = project_id
    self.job_id = job_id if job_id else 'job_%d' % int(time.time())
    self.start = None

  def get_job_ref(self):
    return {'projectId': self.project_id,
            'jobId': self.job_id}

  def start_job(self, job_config):
    '''Given a job configuration, starts the BigQuery job.'''

    body = {
        'jobReference': self.get_job_ref(),
        'configuration' : job_config}
    try:
      with self.lock:
        result = self.bq_service.jobs().insert(
            projectId=self.project_id,
            body=body).execute()
      print json.dumps(result, indent=2)
      return result['jobReference']
    except HttpError, err:
      print 'Error starting job %s:\n%s' % (body, err)
      return None

  def get_job(self):
    job_ref = self.get_job_ref()
    try:
      with self.lock:
        return self.bq_service.jobs().get(
            projectId=job_ref['projectId'],
            jobId=job_ref['jobId']).execute()
    except HttpError, err:
      print 'Error looking up job %s:\n%s' % (job_ref, err)
      return None

  def get_job_state(self):
    job = self.get_job()
    return job['status']['state'] if job else 'ERROR'

  def wait_for_complete(self):
    '''Waits for a BigQuery job to complete.'''
    while True:
      state = self.get_job_state()
      print '%s %ds' % (state, time.time() - self.start)
      if state == 'DONE': break
      time.sleep(5)

    # Print all errors and warnings.
    job = self.get_job()
    for err in job['status'].get('errors', []):
      print json.dumps(err, indent=2)

    # Check for failure.
    if 'errorResult' in job.get('status', {}):
      print 'JOB FAILED'
      print json.dumps(job['status']['errorResult'], indent=2)
      return False
    else:
      print 'JOB COMPLETED'
      return True

