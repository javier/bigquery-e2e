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

def _UserKey(user):
  return ndb.Key("User", user.user_id())

class _DeviceBase(ndb.Model):
  owner = ndb.UserProperty()
  device_id = ndb.StringProperty(indexed=True)

  @classmethod
  def get_by_device_id(cls, device_id):
    return next(iter(cls.query(Candidate.device_id == device_id)), None)
  
class Candidate(_DeviceBase):
  @ndb.transactional(retries=1)
  def register(self, info):
    """This method simply deletes the candidates and creates Device record."""
    device = Device.build(self.device_id, self.owner, info)
    self.key.delete()
    device.put()
    return device

  @classmethod
  def acquire_id(cls, user):
    result = next(iter(cls.query(ancestor=_UserKey(user))), None)
    if not result:
      result = Candidate(id=base64.b64encode(os.urandom(9), '-_'),
                         parent=_UserKey(user))
      result.owner = user
      result.device_id = result.key.id()
      result.put()
    return result.key.id()

    
class Screen(ndb.Model):
  res_x = ndb.IntegerProperty()
  res_y = ndb.IntegerProperty()
  diagonal = ndb.FloatProperty()

class Device(_DeviceBase):
  """Registration record for a device logging to the service."""
  added = ndb.DateTimeProperty(indexed=False, auto_now_add=True)
  type = ndb.StringProperty(indexed=False)
  make = ndb.StringProperty(indexed=False, validator=_validate_str)
  model = ndb.StringProperty(indexed=False, validator=_validate_str)
  os = ndb.StringProperty(indexed=False, choices=('android', 'ios'))
  os_version = ndb.StringProperty(indexed=False, validator=_validate_version)
  storage_gb = ndb.FloatProperty(indexed=False)
  screen = ndb.LocalStructuredProperty(Screen)
  carrier = ndb.StringProperty(indexed=False)
  home_zip5 = ndb.StringProperty(indexed=False, validator=_verify_zip5)

  @classmethod
  def build(cls, device_id, user, info=None):
    result = cls(id=device_id, parent=_UserKey(user))
    result.device_id = device_id
    result.owner = user
    if info:
      result.set(info)
    return result

  @staticmethod
  def new(user, info=None):
    return Device.build(base64.b64encode(os.urandom(9), '-_'), user, info)
  
  @classmethod
  def key_from_id(cls, user, did):
    return ndb.Key(cls, did, parent=_UserKey(user))

  @classmethod
  def by_owner(cls, user):
    return cls.query(ancestor=_UserKey(user))

  def set(self, info):
    self.type = info.get('type')
    self.make = info.get('make')
    self.model = info.get('model')
    self.os = info.get('os')
    self.os_version = info.get('os_version')
    self.storage_gb = float(info.get('storage_gb', 0.0))
    self.screen = _screen_from_params(
        info.get('resolution', '0x0'),
        info.get('screen_size', 0.0))
    self.carrier = info.get('carrier')
    self.home_zip5 = info.get('zip')

_RESOLUTION_RE = re.compile('(\d+)(?:x|X)(\d+)$')
def _screen_from_params(res, dim):
  m = _RESOLUTION_RE.match(res)
  if not m:
    raise ValueError, 'Invalid screen resolution: %s' % res
  return Screen(res_x=int(m.group(1)),
                res_y=int(m.group(2)),
                diagonal=float(dim))
