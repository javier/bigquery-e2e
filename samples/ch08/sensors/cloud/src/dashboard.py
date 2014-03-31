#!/usr/bin/env python

import calendar
from collections import namedtuple
import datetime
import httplib
import json
import logging

import webapp2
from google.appengine.api import taskqueue
from apiclient.errors import HttpError

from config import PROJECT_ID
from config import bigquery

# Structure representing a query to be cached.
BackgroundQuery = namedtuple('BackgroundQuery', [
  'query_job',
  'max_age',
  ])

# Defines a table or query that will be rendered in the dashboard.
ConsoleDataT = namedtuple('ConsoleDataT', [
  'columns',
  'table',  # Set if the data should just be read from the table.
  'query',  # Set if query execution is required.  
  ])

# Scheduled task to trigger dashboard refreshes.
class _Trigger(webapp2.RequestHandler):
  # max_age is the interval be handled.
  def get(self, max_age):
    logging.info("Triggering: " + max_age)
    for index in xrange(len(BACKGROUND_QUERIES)):
      cached = BACKGROUND_QUERIES[int(index)]
      # Only trigger if it is the special value 'all' or if the
      # configuration specifies an interval matching the input interval.
      if (max_age == 'all' or cached.max_age == max_age):
        taskqueue.add(url='/dashboard/update', params={'index': index})
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('ok')

# Handles a dashboard cache update for a given configuration.
class _Update(webapp2.RequestHandler):
  # max_age is the interval be handled.
  def post(self):
    index = self.request.get('index')
    logging.info("Dashboard update: " + index)
    cached = BACKGROUND_QUERIES[int(index)]
    result = bigquery.jobs().insert(
      projectId=PROJECT_ID,
      body=cached.query_job).execute()
    logging.info(str(result))
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('ok')

  # To simplify testing also handle get.
  def get(self):
    self.post()

# Creates logs table for the next 3 days.
class _CreateTableHandler(webapp2.RequestHandler):
  # The get call schedules the post call to create tables.
  def get(self):
    taskqueue.add(url=self.request.route.build(
        self.request, [], {}))
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('ok')
  
  # Performs the actual creation.
  def post(self):
    # First create required datasets.
    for dataset in ['logs', 'dashboard']:
      try:
        bigquery.datasets().insert(
          projectId=PROJECT_ID,
          body={
            'datasetReference': {
              'datasetId': dataset
            }
          }).execute()
      except HttpError, e:
        if e.resp.status == httplib.CONFLICT:
          logging.info('Dataset %s exists' % dataset)
        else:
          logging.error('Error: ' + str(e))

    # Create daily tables for the next 5 days.
    with open('bq/schema_log.json', 'r') as schema_file:
      schema = json.load(schema_file)
    today = datetime.datetime.utcnow()
    for delta in xrange(2, 5):
      day = today + datetime.timedelta(days=delta)
      exp = calendar.timegm(
        (day + datetime.timedelta(days=15)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        .utctimetuple()) * 1000
      request = bigquery.tables().insert(
        projectId=PROJECT_ID,
        datasetId='logs',
        body={
          'tableReference': {
            'tableId': day.strftime('device_%Y%m%d')
          },
          'expirationTime': exp,
          'schema': {
            'fields': schema
          }
        })
      try:
        result = request.execute()
        logging.info('Created table ' + result['id'])
      except HttpError, e:
        if e.resp.status == httplib.CONFLICT:
          logging.info('Table for %s exists' % day)
        else:
          logging.error('Error: ' + str(e))
      
class _Formatter(object):
  '''Base class for formatting rows.'''

  def mime_type(self):
    '''Returns the mime type that the format conforms to.'''
    return 'text/plain'  

  def start(self, out):
    '''Called before the first row is output.'''
    pass
  
  def format(self, out):
    '''Called for each batch of rows to be formatted.'''
    pass

  def finish(self, out):
    '''Called after all rows have been written.'''
    pass

class _CSV(_Formatter):
  '''Format rows as CSV.'''
  def format(self, rows, out):
    out.write('\n'.join([
      ','.join([cell.get('v') for cell in row.get('f')])
      for row in rows]))
    
class _Datatable(_Formatter):
  '''Format data for consumption by the google visualization library.'''
  def __init__(self, config):
    self._columns = json.dumps(config.columns)
    # Need to cast values because the Datatable will treat strings
    # as 0 rather than casting to a number.
    self._converters = [
        float if c['type'] == 'number' else str
        for c in config.columns
      ]
    self._add_comma = False
    
  def _cast(self, cols):
    return [
      {"v":self._converters[i](cols[i]['v'])}
      for i in xrange(len(cols))
    ]

  def mime_type(self):
    return 'application/json'

  def start(self, out):
    out.write('{"cols":' + self._columns + ',\n "rows":[\n')

  def format(self, rows, out):
    if rows:
      if self._add_comma:
        out.write(',\n')
      else:
        self._add_comma = True
      out.write(',\n'.join([
          ('{"c":[' + 
           (','.join([json.dumps(cell) for cell in
                      self._cast(row.get('f'))])) +
           ']}')
          for row in rows]))
  
  def finish(self, out):
    out.write(']}')

class _NextPage(object):
  '''Abstract class for paginating over data.'''
  def fetch(self, token=None, num=10000):
    return self._make_request(token, num).execute()
  
  def _make_request(self, token, num):
    pass
  
class _TableData(_NextPage):
  '''Implements paginating with tabledata().list().'''
  def __init__(self, project, dataset, table):
    self._kwargs = dict(
      projectId=project,
      datasetId=dataset,
      tableId=table)
  
  def _make_request(self, token, num):
    return bigquery.tabledata().list(
      pageToken=token,
      maxResults=num,
      **self._kwargs)

class _QueryResults(_NextPage):
  '''Implements paginating with tabledata().list().'''
  def __init__(self, project, job):
    self._kwargs = dict(
      projectId=project,
      jobId=job)
  
  def _make_request(self, token, num):
    return bigquery.jobs().getQueryResults(
      pageToken=token,
      maxResults=num,
      **self._kwargs)

class _DataHandler(webapp2.RequestHandler):
  def _init_formatter(self, config):
    if self.request.get('format') == 'datatable':
      return _Datatable(config)
    return _CSV()
  
  def get(self, console_id):
    self.response.headers['Cache-Control'] = 'max-age=300'
    console = CONSOLES[int(console_id)]
    formatter = self._init_formatter(console)
    self.response.headers['Content-Type'] = formatter.mime_type()
    if console.table:
      dataset, table = console.table
      next_page = _TableData(PROJECT_ID, dataset, table)
      result = next_page.fetch()
    else:
      result = bigquery.jobs().query(
        projectId=PROJECT_ID,
        body={'query': console.query, 'maxResults': 10000}).execute()
      if 'jobReference' in result:
        job_id = result['jobReference']['jobId']
        next_page = _QueryResults(PROJECT_ID, job_id)
        while 'jobComplete' in result and not result['jobComplete']:
          result = next_page.fetch()
    formatter.start(self.response)
    while result.get('rows'):
      formatter.format(result['rows'], self.response)
      result = (next_page.fetch(result['pageToken']) 
        if 'pageToken' in result else {})
    if result.get('code', 200) != 200:
      self.response.set_status(
        500, message=('Could not fetch data for %s\n%s' % 
          (str(console.table or console.query), json.dumps(result))))
      return
    formatter.finish(self.response)

app = webapp2.WSGIApplication([
    # These URLs should require admin acceess.
    webapp2.Route(r'/dashboard/trigger/<max_age:.*>', handler=_Trigger),
    webapp2.Route(r'/dashboard/update', handler=_Update),
    webapp2.Route(r'/dashboard/create', handler=_CreateTableHandler),
    # These URLs are open.
    webapp2.Route(r'/data/<console_id:\d+>', handler=_DataHandler),
], debug=True)

# Helper function to construct query job configurations.
def _dashboard_query_job(
  query,
  table,
  # Results cached for rendering go in the dashboard dataset.
  dataset='dashboard'):
  return {
      'configuration': {
        'query': {
          'query': query,
          'destinationTable': {
            'projectId': PROJECT_ID,
            'datasetId': dataset,
            'tableId': table
          },
          'writeDisposition': 'WRITE_TRUNCATE'
        }
      }
    }

# List of queries that need to be cached in the dasboard dataset.
BACKGROUND_QUERIES = [
  BackgroundQuery(
    _dashboard_query_job(
      '''SELECT
        SEC_TO_TIMESTAMP(INTEGER(TIMESTAMP_TO_SEC(ts)/60) * 60)
          AS [Minute],
        COUNT(ts) AS [Records],
        SUM(IF(screen_on, 1, 0)) / COUNT(ts) AS [FracScreenOn],
        SUM(IF(power.charging, 1, 0)) / COUNT(ts) AS [FracCharging],
        SUM(IF(power.charge > 0.5, 1, 0)) / COUNT(ts) AS [FracHalfCharged]
      FROM TABLE_DATE_RANGE(logs.device_,
                            DATE_ADD(CURRENT_TIMESTAMP(), -1, 'DAY'),
                            CURRENT_TIMESTAMP())
      WHERE TIMESTAMP_TO_USEC(ts) > (NOW() - 24 * 60 * 60 * 1000 * 1000)
      GROUP BY Minute
      ORDER BY Minute''',
      'records_per_minute'
    ),
    max_age='10m'
  ),
  BackgroundQuery(
    _dashboard_query_job(
      '''SELECT running.name AS app, COUNT(id) AS num
      FROM TABLE_DATE_RANGE(logs.device_,
                            DATE_ADD(CURRENT_TIMESTAMP(), -6, 'DAY'),
                            CURRENT_TIMESTAMP())
      WHERE LEFT(running.name, LENGTH('com.android.')) != 'com.android.'
      AND LEFT(running.name, LENGTH('android.')) != 'android.'
      AND LEFT(running.name, LENGTH('com.google.')) != 'com.google.'
      AND LEFT(running.name, LENGTH('com.motorola.')) != 'com.motorola.'
      AND LEFT(running.name, LENGTH('com.qualcomm.')) != 'com.qualcomm.'
      AND running.name NOT IN (
          'system',
          'com.googlecode.bigquery_e2e.sensors.client',
          'com.redbend.vdmc')
      AND running.importance.level >= 100
      AND running.importance.level < 400
      GROUP BY app
      ORDER BY num DESC''',
      'top_apps'
    ),
    max_age='12h'
  ),
  BackgroundQuery(
    _dashboard_query_job(
      '''SELECT ZipsInDay, COUNT(1) FROM (
        SELECT D, id, COUNT(zip) AS ZipsInDay FROM (
          SELECT
            DATE(ts) AS D, id, location.zip AS zip
          FROM TABLE_DATE_RANGE(logs.device_,
                                DATE_ADD(CURRENT_TIMESTAMP(), -6, 'DAY'),
                                CURRENT_TIMESTAMP())
          GROUP EACH BY D, id, zip)
        GROUP EACH BY D, id)
      GROUP BY ZipsInDay ORDER BY ZipsInDay''',
      'zips_in_day'
    ),
    max_age='12h'
  ),
]

def ConsoleData(columns, table=None, query=None):
  assert not(table and query)
  assert table or query
  return ConsoleDataT(columns, table=table, query=query)

CONSOLES = [
  ConsoleData(
    [
      {'label': 'Minute', 'type': 'number'},
      {'label': 'Records', 'type': 'number'},
    ],
    query=(
      '''SELECT Minute, Records
      FROM dashboard.records_per_minute
      ORDER BY Minute''')
  ),
  ConsoleData(
    [
      {'label': 'Minute', 'type': 'number'},
      {'label': 'Screen On', 'type': 'number'},
      {'label': 'Charging', 'type': 'number'},
      {'label': 'Half Charged', 'type': 'number'},
    ],
    query=(
      '''SELECT Minute, FracScreenOn, FracCharging, FracHalfCharged 
      FROM dashboard.records_per_minute
      ORDER BY Minute''')
  ),
  ConsoleData(
    [
      {'label': 'Application', 'type': 'string'},
      {'label': 'Users', 'type': 'number'},
    ],
    table=('dashboard', 'top_apps')
  ),
  ConsoleData(
    [
      {'label': 'Zips In One Day', 'type': 'number'},
      {'label': 'Num Device Days', 'type': 'number'},
    ],
    table=('dashboard', 'zips_in_day')
  ),
]