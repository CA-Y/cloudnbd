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
import threading
from cnbdcore.cloud import *

# set the boto config that forces certificate validation for HTTPS
# connections

from boto import config
if not config.has_section('Boto'):
  config.add_section('Boto')
config.setbool('Boto', 'https_validate_certificates', True)

class GSObject(CloudObject):
  """A cloud object with content and metadata, initially however
  containing only the metadata.
  """

  def __init__(self, parent, nativeobj):
    self._nativeobj = nativeobj
    self._parent = parent
    self.metadata = nativeobj.metadata

  def get_content(self):
    """Download the content of the this object."""
    while True:
      try:
        return self._nativeobj.get_contents_as_string()
      except: # XXX we might want to only catch certain errors here
        pass 
      time.sleep(1)

class GS(Bridge):
  """Web service interface"""
  def __init__(self, *args, **kwargs):
    Bridge.__init__(self, *args, **kwargs)

  def check_access(self):
    """Determine whether this instance with the given credentials is
    able to access the storage.
    """
    self._gsbucket, self._gsvolume = self.volume.split('/', 1)
    from boto.gs.connection import GSConnection
    from boto.exception import GSResponseError
    try:
      self._conn = GSConnection(self.access_key, self.secret_key)
      self._bucket = self._conn.get_bucket(self._gsbucket)
    except GSResponseError as e:
      if e.error_code == 'NoSuchBucket':
        raise BridgeInvalidVolume('Invalid bucket name')
      else: # e.error_code == 'AccessDenied':
        raise BridgeAccessDenied('Invalid access key specified')
    self._can_access = True

  def clone(self):
    new_gs = Bridge.clone(self, base=GS)
    if new_gs._can_access:
      new_gs._gsbucket, new_gs._gsvolume = new_gs.volume.split('/', 1)
      from boto.gs.connection import GSConnection
      from boto.gs.bucket import Bucket
      new_gs._conn = GSConnection(self.access_key, self.secret_key)
      new_gs._bucket = Bucket(
        connection=new_gs._conn,
        name=new_gs._gsbucket
      )
    return new_gs

  def get(self, path):
    """Get the value of the object given by the path."""
    self._ensure_access()
    while True:
      try:
        key = self._bucket.get_key('%s/%s' % (self._gsvolume, path))
        break
      except: # XXX maybe specify some exceptions here
        pass
      time.sleep(1)
    if key:
      return GSObject(parent=self, nativeobj=key)
    else:
      return None

  def set(self, path, content, metadata={}):
    """Set the value of the object given by the path."""
    self._ensure_access()
    from boto.gs.key import Key
    k = Key(self._bucket)
    k.key = '%s/%s' % (self._gsvolume, path)
    k.metadata = metadata
    while True:
      try:
        k.set_contents_from_string(content)
        break
      except: # XXX maybe specify some exceptions here
        pass
      time.sleep(1)

  def delete(self, path):
    from boto.exception import GSResponseError
    self._ensure_access()
    while True:
      try:
        self._bucket.delete_key('%s/%s' % (self._gsvolume, path))
        break
      except GSResponseError as e:
        if e.error_code == 'NoSuchKey':
          break
      except:
        pass
      time.sleep(1)

  def list(self, prefix=''):
    self._ensure_access()
    while True:
      try:
        return self._bucket.list(prefix='%s/%s'
                                 % (self._gsvolume, prefix))
      except:
        pass
      time.sleep(1)
