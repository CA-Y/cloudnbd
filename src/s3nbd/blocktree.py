#!/usr/bin/env python
#
# blocktree.py - Interface between S3/Local cache and the high level
#                logic of S3BD
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
from s3nbd.cmdlind import fatal, warning, info

class BlockTree(object):
  """Interface between S3/Local cache and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, s3 = None):
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.s3 = s3

  def set(self, path, data, direct = False):
    """Set the value of an object locally and optionally remotely. If
    direct is set to True, the data is immediately uploaded to S3.
    """
    chksum = self._build_checksum(path, data)
    cryptdata = self._encrypt_data(path, data)
    self._set_local(path, cryptdata)
    self._set_checksum(path, chksum)
    if direct:
      self._upload(path, cryptdata, chksum)
    else:
      self._append_trans(path, cryptdata)

  def get(self, path, direct = False):
    if direct:
      return self._download(path)
    cryptdata = self._load_local(path)
    if cryptdata is None:
      if path in self._transaction:
        fatal('local copy of a in-transaction block is missing')
      return self._download(path)
    else:
      plaindata = self._decrypt_data(path, cryptdata)
      local_chksum = self._get_checksum(path)
      if local_chksum is None:
        self._set_checksum(self._s3.get(path).metadata['chksum'])
        local_chksum = self._get_checksum(path)
      chksum = self._build_checksum(path, plaindata)
      if chksum != local_chksum:
        return self._download(path)
      else:
        return plaindata
