#!/usr/bin/env python
#
# __init__.py - Cloud service interface
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

class BridgeError(Exception):
  pass
class BridgeAccessDenied(BridgeError):
  pass
class BridgeNoSuchBucket(BridgeError):
  pass
class BridgeAccessNotChecked(BridgeError):
  pass

class CloudObject(object):
  """A cloud object with content and metadata, initially however
  containing only the metadata.
  """

  def __init__(self, parent, nativeobj):
    self._nativeobj = nativeobj
    self._parent = parent

  def get_content(self):
    """Download the content of the this object."""
    raise NotImplementedError('abstract class')

class Bridge(object):
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
    raise NotImplementedError('abstract class')

  def clone(self):
    raise NotImplementedError('abstract class')

  def get(self, path):
    """Get the value of the object given by the path."""
    raise NotImplementedError('abstract class')

  def set(self, path, content, metadata={}):
    """Set the value of the object given by the path."""
    raise NotImplementedError('abstract class')

  def copy(self, src, target):
    raise NotImplementedError('abstract class')

  def delete(self, path):
    raise NotImplementedError('abstract class')

from cloudnbd.cloud import gs

backends = {
  'gs': gs.GS,
  's3': None
}
