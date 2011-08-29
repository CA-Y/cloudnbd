#!/usr/bin/env python
#
# s3.py - Amazon S3 web service interface
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
import s3nbd

class S3AccessDenied(Error):
  pass
class S3NoSuchBucket(Error):
  pass

class S3Object(object):
  """An S3 object with content and metadata, initially however
  containing only the metadata.
  """

  def __init__(self, parent, s3key):
    self._parent = parent
    self._s3key = s3key
    self.metadata = s3key.metadata

  def get_content(self):
    """Download the content of the this object."""
    import time
    with self._parent._lock:
      while True:
        try:
          return self._s3key.get_contents_as_string()
        except: # XXX we might want to only catch certain errors here
          pass 
        time.sleep(1)

class S3(object):
  """Amazon S3 web service interface"""
  def __init__(self, access_key = None, secret_key = None,
               bucket = None, volume = None):
    import threading
    self.access_key = access_key
    self.secret_key = secret_key
    self.bucket = bucket
    self.volume = volume
    self._can_access = False
    self._lock = threading.RLock()

  def check_access(self):
    """Determine whether this S3 instance with the given credentials is
    able to access the storage.
    """
    from boto.s3.connection import S3Connection
    from boto.exception import S3ResponseError
    try:
      self._conn = S3Connection(self.access_key, self.secret_key)
      self._bucket = self._conn.get_bucket(self.bucket)
    except S3ResponseError as e:
      if e.error_code == 'NoSuchBucket':
        raise S3NoSuchBucket('Invalid bucket name given')
      else: # e.error_code == 'AccessDenied':
        raise S3AccessDenied('Invalid access key or secret for the'
                             ' specified bucket')
    self._can_access = True

  def get(self, path):
    import time
    with self._lock:
      while True:
        try:
          key = self._bucket.get_key('%s/%s'
            % (self.volume, path.strip('/')))
        except: # XXX maybe specify some exceptions here
          pass
        time.sleep(1)
    return S3Object(parent=self, s3key=key)

  def set(self, path, content, metadata={}):
    pass
  
