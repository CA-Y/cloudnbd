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
import cnbdcore
import time
import threading

class BridgeError(Exception):
  pass
class BridgeAccessDenied(BridgeError):
  pass
class BridgeInvalidVolume(BridgeError):
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
  def __init__(self, access_key = None, secret_key = None,
               volume = None):
    self.access_key = access_key
    self.secret_key = secret_key
    self.volume = volume
    self._can_access = False

  def check_access(self):
    """Determine whether this instance with the given credentials is
    able to access the storage.
    """
    raise NotImplementedError('abstract class')

  def clone(self, base = None):
    if base is None:
      base = Bridge
    new_bridge = base()
    new_bridge.access_key = self.access_key
    new_bridge.secret_key = self.secret_key
    new_bridge.volume = self.volume
    new_bridge._can_access = self._can_access
    return new_bridge

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

  def list(self, prefix):
    raise NotImplementedError('abstract class')

  def _ensure_access(self):
    if not self._can_access:
      raise BridgeAccessNotChecked(
        'check_access() must be called first'
      )

from cnbdcore.cloud import gs

backends = {
  # 's3': None,
  # 'fs': None,
  'gs': gs.GS
}
