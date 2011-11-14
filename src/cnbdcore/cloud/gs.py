#!/usr/bin/env python
#
# gs.py - Google Storage interface
# Copyright (C) 2011  Mansour <mansour@oxplot.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
import cnbdcore
import time
import signal
from multiprocessing import Queue, Process
from cnbdcore.cloud import *

# set the boto config that forces certificate validation for HTTPS
# connections

from boto import config
if not config.has_section('Boto'):
  config.add_section('Boto')
config.setbool('Boto', 'https_validate_certificates', True)

_wdt_timeout = 5 * 60 # 5 minute timeout

class GSObject(CloudObject):
  """A cloud object with content and metadata, initially however
  containing only the metadata.
  """

  def __init__(self, parent, nativeobj):
    self._path, self.metadata = nativeobj
    self._parent = parent
    # run a separate thread for all network activities
    self._thread = Process

  def get_content(self):
    """Download the content of the this object."""
    return self._parent._get_contents(self._path)

class GS(Bridge):
  """Web service interface"""
  def __init__(self, *args, **kwargs):
    Bridge.__init__(self, *args, **kwargs)
    self._request = Queue(1)
    self._response = Queue(1)

  def _worker(self):
    try:
      self._worker_helper()
    except:
      pass

  def _worker_helper(self):
    self._lists = {}
    self._next_list_id = 0
    while True:
      req = self._request.get()
      req, args = req[0], req[1:]
      if req == 'check_access':
        try:
          self._worker_check_access(args[0])
        except Exception as e:
          self._response.put(e)
        else:
          self._response.put(True)
      elif req == 'get':
        self._response.put(self._worker_get(args[0]))
      elif req == 'get_contents':
        self._response.put(self._worker_get_contents(args[0]))
      elif req == 'set':
        self._response.put(self._worker_set(*args))
      elif req == 'delete':
        self._response.put(self._worker_delete(args[0]))
      elif req == 'list':
        self._lists[self._next_list_id] = \
          self._worker_list(args[0]).__iter__()
        self._response.put(self._next_list_id)
        self._next_list_id += 1
      elif req == 'list-item':
        try:
          next_item = self._lists[args[0]].next()
        except Exception as e:
          if isinstance(e, StopIteration):
            del self._lists[args[0]]
          self._response.put(e)
        else:
          self._response.put(next_item.name)

  def _wdt(self, f, *args, **kwargs):
    def handler(signum, frame):
      raise Exception()
    old = signal.signal(signal.SIGALRM, handler)
    signal.siginterrupt(signal.SIGALRM, True)
    signal.alarm(_wdt_timeout)
    try:
      return f(*args, **kwargs)
    finally:
      signal.signal(signal.SIGALRM, old)
      signal.alarm(0)

  def _worker_check_access(self, as_clone):
    from boto.gs.connection import GSConnection
    from boto.exception import GSResponseError
    from boto.gs.bucket import Bucket
    try:
      self._conn = GSConnection(self.access_key, self.secret_key)
      if as_clone:
        self._bucket = Bucket(
          connection=self._conn, name=self._gsbucket
        )
      else:
        self._bucket = self._conn.get_bucket(self._gsbucket)
    except GSResponseError as e:
      if e.error_code == 'NoSuchBucket':
        raise BridgeInvalidVolume('Invalid bucket name')
      else: # e.error_code == 'AccessDenied':
        raise BridgeAccessDenied('Invalid access key specified')

  def _worker_get(self, path):
    while True:
      try:
        key = self._wdt(self._bucket.get_key, path)
        break
      except: # XXX maybe specify some exceptions here
        pass
      time.sleep(1)
    if key:
      return (path, key.metadata)
    else:
      return None

  def _worker_get_contents(self, path):
    from boto.gs.key import Key
    key = Key(bucket=self._bucket)
    key.key = path
    while True:
      try:
        return self._wdt(key.get_contents_as_string)
      except:
        pass

  def _get_contents(self, path):
    self._request.put(('get_contents', path))
    return self._response.get()

  def _worker_set(self, path, content, metadata):
    from boto.gs.key import Key
    k = Key(self._bucket)
    k.key = path
    k.metadata = metadata
    while True:
      try:
        self._wdt(k.set_contents_from_string, content)
        break
      except: # XXX maybe specify some exceptions here
        pass
      time.sleep(1)

  def _worker_delete(self, path):
    from boto.exception import GSResponseError
    while True:
      try:
        self._wdt(self._bucket.delete_key, path)
        break
      except GSResponseError as e:
        if e.error_code == 'NoSuchKey':
          break
      except:
        pass
      time.sleep(1)

  def _worker_list(self, prefix):
    while True:
      try:
        return self._wdt(self._bucket.list, prefix=prefix)
      except:
        pass
      time.sleep(1)

  def check_access(self, as_clone = False):
    """Determine whether this instance with the given credentials is
    able to access the storage.
    """
    if self._can_access and not as_clone:
      return
    self._gsbucket, self._gsvolume = self.volume.split('/', 1)
    if not hasattr(self, '_process'):
      self._process = Process(target=self._worker)
      self._process.daemon = True
      self._process.start()
    self._request.put(('check_access', as_clone))
    res = self._response.get()
    if isinstance(res, Exception):
      raise res
    self._can_access = True

  def clone(self):
    new_gs = Bridge.clone(self, base=GS)
    new_gs.check_access(as_clone=True)
    return new_gs

  def get(self, path):
    """Get the value of the object given by the path."""
    self._ensure_access()
    self._request.put(('get', '%s/%s' % (self._gsvolume, path)))
    res = self._response.get()
    if isinstance(res, Exception):
      raise res
    if not res:
      return None
    return GSObject(parent=self, nativeobj=res)

  def set(self, path, content, metadata={}):
    """Set the value of the object given by the path."""
    self._ensure_access()
    self._request.put(('set', '%s/%s' % (self._gsvolume, path),
                       content, metadata))
    res = self._response.get()
    if isinstance(res, Exception):
      raise res

  def delete(self, path):
    self._ensure_access()
    self._request.put(('delete', '%s/%s' % (self._gsvolume, path)))
    res = self._response.get()
    if isinstance(res, Exception):
      raise res

  def list(self, prefix=''):
    self._ensure_access()
    self._request.put(('list', '%s/%s' % (self._gsvolume, prefix)))
    res = self._response.get()
    if isinstance(res, Exception):
      raise res
    class _iter_list(object):
      def next(self):
        self._parent._request.put(('list-item', self._id))
        res = self._parent._response.get()
        if isinstance(res, Exception):
          raise res
        return res
      def __iter__(self):
        return self
    il = _iter_list()
    il._parent = self
    il._id = res
    return il
