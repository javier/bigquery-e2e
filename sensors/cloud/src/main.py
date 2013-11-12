#!/usr/bin/env python
import os.path

import jinja2
import webapp2
import httplib2
from oauth2client.appengine import AppAssertionCredentials
from apiclient.discovery import build
from google.appengine.api import users
from google.appengine.ext.webapp.util import login_required
from google.appengine.api import memcache
from google.appengine.api import app_identity

import models

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


_PROJECT_ID = 'bigquery-e2e'

access_token = app_identity.get_access_token(
    'https://www.googleapis.com/auth/bigquery')
print access_token
if access_token[0].find('InvalidToken') > -1:
  from dev_auth.auth import get_bigquery
  bigquery = get_bigquery()
else:
  credentials = AppAssertionCredentials(
      scope='https://www.googleapis.com/auth/bigquery')
  bigquery = build('bigquery', 'v2',
                   http=credentials.authorize(httplib2.Http(memcache)))

datasets = bigquery.datasets()
tables = bigquery.tables()

class MainHandler(webapp2.RequestHandler):
  def get(self):
    template = JINJA_ENVIRONMENT.get_template('templates/index.html')
    response = tables.list(projectId=_PROJECT_ID, datasetId='reference').execute()
    
    self.response.write(template.render({
      'tables': [{
          'dataset': entry['tableReference']['datasetId'],
          'name': entry['tableReference']['tableId'],
      } for entry in response['tables']]}))

class ManageDevicesHandler(webapp2.RequestHandler):
  @login_required
  def get(self):
    user = users.get_current_user()
    self._show(user)

  def _show(self, user):
    data = {
      'user': user,
      'devices': [{
         'id': device.key.id(),
         'added': device.added.strftime('%c'),
         'make': device.make,
         'model': device.model,
         'zip': device.home_zip5,
        } for device in models.Device.by_owner(user)],
    }
    template = JINJA_ENVIRONMENT.get_template('templates/manage.html')
    self.response.write(template.render(data))

  def post(self):
    user = users.get_current_user()
    if user is None:
      self.abort(401, 'Must be logged in to manage devices.')
    action = self.request.get('action')
    if action == 'Add':
      self._handle_add(user)
    elif action == 'X':
      self._handle_remove()
    else:
      self.abort(400, detail=('Unsupported action %s' % action))
    self.redirect(self.request.route.build(
        self.request, [], {}))

  def _handle_add(self, user):
    device = models.NewDevice()
    device.owner = user
    device.type = self.request.get('type')
    device.make = self.request.get('make')
    device.model = self.request.get('model')
    device.os = self.request.get('os')
    device.os_version = self.request.get('os_version')
    device.storage_gb = float(self.request.get('storage_gb'))
    device.screen = models.ScreenFromParams(
        self.request.get('resolution'),
        self.request.get('screen_size'))
    device.carrier = self.request.get('carrier')
    device.home_zip5 = self.request.get('zip')
    device.put()

  def _handle_remove(self):
    models.Device.key_from_id(self.request.get('id')).delete()
    
app = webapp2.WSGIApplication([
    webapp2.Route(r'/', handler=MainHandler, name='main'),
    webapp2.Route(r'/manage', handler=ManageDevicesHandler, name='manage'),
], debug=True)
    