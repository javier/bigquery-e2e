import base64
import os
import re
from google.appengine.ext import ndb

_ZIP5_RE = re.compile('\d{5}$')
def _verify_zip5(prop, value):
  if not _ZIP5_RE.match(value):
    raise ValueError('Invalid zip5 code: ' + value)

def _validate_str(prop, v):
  return v.strip().lower()

_VERSION_RE = re.compile('\d\d?(?:\.\d\d?){0,2}$')
def _validate_version(prop, v):
  if _VERSION_RE.match(v):
    return None
  raise ValueError, 'Version must match 9[9][.9[9]][.9[9]]'

class Screen(ndb.Model):
  res_x = ndb.IntegerProperty()
  res_y = ndb.IntegerProperty()
  diagonal = ndb.FloatProperty()

class Device(ndb.Model):
  """Registration record for a device logging to the service."""
  owner = ndb.UserProperty()
  added = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  type = ndb.StringProperty(indexed=False, choices=('phone', 'tablet'))
  make = ndb.StringProperty(indexed=False, validator=_validate_str)
  model = ndb.StringProperty(indexed=False, validator=_validate_str)
  os = ndb.StringProperty(indexed=False, choices=('android', 'ios'))
  os_version = ndb.StringProperty(indexed=False, validator=_validate_version)
  storage_gb = ndb.FloatProperty(indexed=False)
  screen = ndb.LocalStructuredProperty(Screen)
  carrier = ndb.StringProperty(indexed=False)
  home_zip5 = ndb.StringProperty(indexed=False, validator=_verify_zip5)
  
  @classmethod
  def key_from_id(cls, did):
    return ndb.Key(cls, did)

  @classmethod
  def by_owner(cls, user):
    return cls.query(cls.owner == user)

def NewDevice():
  return Device(id=base64.b64encode(os.urandom(9), '-_'))

_RESOLUTION_RE = re.compile('(\d+)(?:x|X)(\d+)$')
def ScreenFromParams(res, dim):
  m = _RESOLUTION_RE.match(res)
  if not m:
    raise ValueError, 'Invalid screen resolution: %s' % res
  return Screen(res_x=int(m.group(1)),
                res_y=int(m.group(2)),
                diagonal=float(dim))
