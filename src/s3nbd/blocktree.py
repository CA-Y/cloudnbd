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
import os
import struct
import zlib
import hashlib
import time
import threading
from Crypto.Cipher import AES

class BTError(Exception):
  pass
class BTDecryptFailed(BTError):
  pass
class BTChecksumError(BTError):
  pass

def _writer_factory(blocktree):
  s3 = blocktree.s3.clone()
  def writer():
    try:
      while True:
        path, data = blocktree._cache.dequeue()
        checksum = blocktree._build_checksum(path, data)
        data = blocktree._encrypt_data(path, data)
        s3.set(path, data, metadata={'checksum': checksum})
        del data
    except s3nbd.QueueEmptyError:
      pass
  return writer

class BlockTree(object):
  """Interface between S3/Local cache and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, s3 = None,
               threads = 1, cow = False):
    self.cow = cow
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.s3 = s3
    self._cache = s3nbd.Cache()
    self._cache.queue_size = s3nbd._default_write_cache_size
    self._cache.total_size = s3nbd._default_total_cache_size
    self._cache.backercb = self._cache_read_cb
    # initialize the writer threads
    self._writers = []
    for i in xrange(threads):
      writer = threading.Thread(target=_writer_factory(self))
      writer.daemon = True
      self._writers.append(writer)
      writer.start()

  def _cache_read_cb(self, k):
    obj = self.s3.get(k)
    if obj:
      data = self._decrypt_data(k, obj.get_content())
      s3_checksum = obj.metadata['checksum']
      calc_checksum = self._build_checksum(k, data)
      if s3_checksum != calc_checksum:
        raise BTChecksumError(
         "S3 and calculated checksums for object:%s don't match" % k
        )
      return data
    else:
      return None

  def set(self, path, data, direct = False):
    """Upload/queue an object on/to be uploaded to S3."""
    if direct:
      checksum = self._build_checksum(path, data)
      cryptdata = self._encrypt_data(path, data)
      self.s3.set(path, cryptdata, metadata={'checksum': checksum})
    else:
      self._cache[path] = data

  def _build_checksum(self, path, data):
    """Calculate the checksum for given path anda data."""
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.sha256(s3nbd._salt + key
      + path.encode('utf8') + (b'' if data is None else data))
    return hasher.hexdigest()

  def _decrypt_data(self, path, data):
    """Decrypt the given data."""
    if not data:
      return None
    zipped, size = struct.unpack_from(b'!BQ', data, 0)
    data = data[struct.calcsize(b'!BQ'):]
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.md5(s3nbd._salt + path.encode('utf8'))
    iv = hasher.digest()
    decryptor = AES.new(key, AES.MODE_CBC, iv)
    data = decryptor.decrypt(data)
    data = data[:size]
    if zipped:
      data = zlib.decompress(data)
    return data

  def _encrypt_data(self, path, data):
    """Encrypt the given data."""
    if not data:
      return None
    zipped = zlib.compress(data)
    if len(zipped) < len(data):
      storezip = 1
      data = zipped
    else:
      storezip = 0
    header = struct.pack(b'!BQ', storezip, len(data))
    data = data.ljust((len(data) // 32 + 1) * 32, b'\0')
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.md5(s3nbd._salt + path.encode('utf8'))
    iv = hasher.digest()
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    data = encryptor.encrypt(data)
    return header + data

  def _cow_path(self, path):
    if self.cow and path.startswith('blocks/'):
      return 'cow' + path
    else:
      return path

  def get(self, path):
    """Get the value of an object."""
    return self._cache[path]

  def close(self):
    self._cache.set_wait_on_empty(False)
    for th in self._writers:
      th.join()
