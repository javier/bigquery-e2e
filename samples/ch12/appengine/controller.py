#!/usr/bin/env python
import cgi
import time
import threading
import json

from google.appengine.api import users
from google.appengine.ext.webapp.util import login_required
from google.appengine.api import memcache
from google.appengine.api import app_identity
from google.appengine.api import background_thread
import webapp2
import httplib2
from oauth2client.appengine import AppAssertionCredentials
from apiclient.discovery import build
from mapreduce.mapper_pipeline import MapperPipeline

_PROJECT_ID = 'bigquery-e2e'

access_token = app_identity.get_access_token(
    'https://www.googleapis.com/auth/bigquery')
if access_token[0].find('InvalidToken') > -1:
  from dev_auth.auth import get_bigquery
  bigquery = get_bigquery()
else:
  credentials = AppAssertionCredentials(
      scope='https://www.googleapis.com/auth/bigquery')
  bigquery = build('bigquery', 'v2',
                   http=credentials.authorize(httplib2.Http(memcache)))

jobs = bigquery.jobs()
PROJECT_ID = 317752944021
GCS_BUCKET = 'bigquery-e2e'

state_lock = threading.RLock()
ZERO_STATE = {
  'status': 'IDLE',
  'extract_job_id': '',
  'extract_result': '',
  'load_job_id': '',
  'load_result': '',
  'mapper_link': '',
  'error': 'None',
  'refresh': '',
  }
state = ZERO_STATE.copy()

def Pre(s):
  return '<pre>' + cgi.escape(str(s)) + '</pre>'

def RunBigqueryJob(job_id_prefix, job_type, config):
  job_id = job_id_prefix + '_' + job_type
  with state_lock:
    state[job_type + '_job_id'] = job_id
  body = {
    'jobReference': {
      'jobId': job_id
      },
    'configuration': {
      job_type: config
      }
    }
  result = jobs.insert(projectId=PROJECT_ID, body=body).execute()
  print result
  while True:
    with state_lock:
      state[job_type + '_result'] = Pre(json.dumps(result, indent=2))
    if result['status']['state'] == 'DONE':
      break
    time.sleep(5)
    result = jobs.get(projectId=PROJECT_ID, jobId=job_id).execute()
  if 'errorResult' in result['status']:
    raise RuntimeError(json.dumps(result['status']['errorResult'], indent=2))

def WaitForPipeline(pipeline_id):
  mapreduce_id = None
  while True:
    time.sleep(5)
    pipeline = MapperPipeline.from_id(pipeline_id)
    if not mapreduce_id and pipeline.outputs.job_id.filled:
      mapreduce_id = pipeline.outputs.job_id.value
      with state_lock:
        state['mapper_link'] = (
          '<a href="/mapreduce/detail?mapreduce_id=%s">%s</a>' % (
            mapreduce_id, mapreduce_id))
    if pipeline.has_finalized:
      break
  if pipeline.outputs.result_status.value != 'success':
    raise RuntimeError('Mapper job failed, see status link.')
  
def TableReference(table_id):
  return {
    'projectId': PROJECT_ID,
    'datasetId': 'ch12',
    'tableId': table_id,
    }

OUTPUT_SCHEMA = {
  'fields': [
    {'name':'id', 'type':'STRING'},
    {'name':'lat', 'type':'FLOAT'},
    {'name':'lng', 'type':'FLOAT'},
    {'name':'zip', 'type':'STRING'},
    ]
  }

def RunTransform():
  JOB_ID_PREFIX = 'ch12_%d' % int(time.time())
  TMP_PATH = 'tmp/mapreduce/%s' % JOB_ID_PREFIX

  # Extract from BigQuery to GCS.
  RunBigqueryJob(JOB_ID_PREFIX, 'extract', {
      'sourceTable': TableReference('add_zip_input'),
      'destinationUri': 'gs://%s/%s/input-*' % (GCS_BUCKET, TMP_PATH),
      'destinationFormat': 'NEWLINE_DELIMITED_JSON',
      })

  # Run the mapper job to annotate the records.
  mapper = MapperPipeline(
    'Add Zip',
    'add_zip.apply',
    'mapreduce.input_readers.FileInputReader',
    'mapreduce.output_writers._GoogleCloudStorageOutputWriter',
    params={
      'files': ['/gs/%s/%s/input-*' % (GCS_BUCKET, TMP_PATH)],
      'format': 'lines',
      'output_writer': {
        'bucket_name': GCS_BUCKET,
        'naming_format': TMP_PATH + '/output-$num',
        }
      })
  mapper.start()
  WaitForPipeline(mapper.pipeline_id)

  # Load from GCS into BigQuery.
  RunBigqueryJob(JOB_ID_PREFIX, 'load', {
      'destinationTable': TableReference('add_zip_output'),
      'sourceUris': ['gs://%s/%s/output-*' % (GCS_BUCKET, TMP_PATH)],
      'sourceFormat': 'NEWLINE_DELIMITED_JSON',
      'schema': OUTPUT_SCHEMA,
      'writeDisposition': 'WRITE_TRUNCATE',
      })

def RunAttempt():
  global state
  try:
    with state_lock:
      if state['status'] == 'RUNNING':
        return
      state = ZERO_STATE.copy()
      state['status'] = 'RUNNING'
    RunTransform()
  except Exception, err:
    with state_lock:
      state['error'] = Pre(err)
  finally:
    with state_lock:
      state['status'] = 'IDLE'

class MainHandler(webapp2.RequestHandler):
  @login_required
  def get(self):
    current = ZERO_STATE.copy()
    with state_lock:
      current.update(state)
      if current['status'] == 'RUNNING':
        current['refresh'] = '<meta http-equiv="refresh" content="6"/>'
    self.response.write(_PAGE % current)

  def post(self):
    if not users.is_current_user_admin():
      self.abort(401, 'Must be an admin to start a mapreduce.')    
    background_thread.start_new_background_thread(RunAttempt, [])
    self.redirect(self.request.route.build(self.request, [], {}))

app = webapp2.WSGIApplication([
    webapp2.Route(r'/', handler=MainHandler, name='main'),
], debug=True)

_PAGE = '''<html>
<head>
<title>MapReduce controller</title>
%(refresh)s
</head>
<body>
<h1>MapReduce Controller</h1>
<div><a href="/mapreduce/status">MapReduce Status</a></div>
<div>Status: %(status)s</div>
<div>
Extract Job Id: %(extract_job_id)s
%(extract_result)s
</div>
<div>
Mapper: %(mapper_link)s
</div>
<div>
Load Job Id: %(load_job_id)s
%(load_result)s
</div>
<div>%(error)s</div>
<div>
<form action="/" method="post">
  <input type="submit" name="action" value="Start"/>
</form>
</div>
</body>
</html>'''
