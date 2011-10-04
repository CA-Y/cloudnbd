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
import cloudnbd
import time
import threading
import urllib2
import hashlib
from cloudnbd.cloud import *

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
  def __init__(self, access_key = None, bucket = None, volume = None):
    self.access_key = access_key
    self.bucket = bucket
    self.volume = volume
    self._can_access = False

  def check_access(self):
    """Determine whether this instance with the given credentials is
    able to access the storage.
    """
    self._access_key, self._secret_key = self.access_key.split(':')
    from boto.gs.connection import GSConnection
    from boto.exception import GSResponseError
    try:
      self._conn = GSConnection(self._access_key, self._secret_key)
      self._bucket = self._conn.get_bucket(self.bucket)
    except GSResponseError as e:
      if e.error_code == 'NoSuchBucket':
        raise BridgeNoSuchBucket('Invalid bucket name given')
      else: # e.error_code == 'AccessDenied':
        raise BridgeAccessDenied('Invalid access key specified bucket')
    self._can_access = True

  def clone(self):
    new_gs = GS()
    new_gs.access_key = self.access_key
    new_gs._access_key = self._access_key
    new_gs._secret_key = self._secret_key
    new_gs.bucket = self.bucket
    new_gs.volume = self.volume
    new_gs._can_access = self._can_access
    if new_gs._can_access:
      from boto.gs.connection import GSConnection
      from boto.gs.bucket import Bucket
      new_gs._conn = GSConnection(self._access_key, self._secret_key)
      new_gs._bucket = Bucket(
        connection=new_gs._conn,
        name=new_gs.bucket
      )
    return new_gs

  def get(self, path):
    """Get the value of the object given by the path."""
    self._ensure_access()
    while True:
      try:
        key = self._bucket.get_key('%s/%s' % (self.volume, path))
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
    k.key = '%s/%s' % (self.volume, path)
    k.metadata = metadata
    while True:
      try:
        k.set_contents_from_string(content)
        break
      except: # XXX maybe specify some exceptions here
        pass
      time.sleep(1)

  def copy(self, src, target):
    self._ensure_access()
    while True:
      try:
        self._bucket.copy_key(
          '%s/%s' % (self.volume, new_path),
          self.bucket,
          '%s/%s' % (self.volume, old_path)
        )
        break
      except:
        pass
      time.sleep(1)

  def delete(self, path):
    from boto.exception import GSResponseError
    self._ensure_access()
    while True:
      try:
        self._bucket.delete_key('%s/%s' % (self.volume, path))
        break
      except GSResponseError as e:
        if e.error_code == 'NoSuchKey':
          break
      time.sleep(1)

  def list(self, prefix=''):
    self._ensure_access()
    while True:
      try:
        return self._bucket.list(prefix='%s/%s' % (self.volume, prefix))
      except:
        pass
      time.sleep(1)

  def _ensure_access(self):
    if not self._can_access:
      raise BridgeAccessNotChecked(
        'check_access() must be called first'
      )
