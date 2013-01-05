#!/usr/bin/env python
#
# blocktree.py - Interface between cloud and high level logic
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
import os
import struct
import zlib
import hashlib
import time
import threading
import re
from Crypto.Cipher import AES

class BTError(Exception):
  pass
class BTInvalidKey(BTError):
  pass
class BTChecksumError(BTError):
  pass

def _writer_factory(blocktree):
  cloud = blocktree.cloud.clone()
  def writer():
    try:
      while True:
        path, data = blocktree._cache.dequeue()
        if data is None:
          cloud.delete(path)
          with blocktree._stats_lock:
            blocktree._stats['deleted_count'] += 1
        else:
          checksum = blocktree._build_checksum(path, data)
          plain_data_len = len(data)
          data = blocktree._encrypt_data(path, data)
          cloud.set(path, data, metadata={'checksum': checksum})
          with blocktree._stats_lock:
            blocktree._stats['sent_count'] += 1
            blocktree._stats['data_sent'] += plain_data_len
            blocktree._stats['wire_sent'] += len(data)
          blocktree._cache.unpin(path)
          del data
    except cloudnbd.QueueEmptyError:
      pass
  return writer

def _reader_factory(blocktree):
  cloud = blocktree.cloud.clone()
  def reader():
    try:
      while True:
        k = blocktree._read_queue.pop()
        if k in blocktree._cache:
          continue
        try:
          value = _indep_get(blocktree, cloud, k)
        except BTChecksumError:
          continue # XXX since we're using reader only for read ahead
                   #     it's ok to ignore checksum issues
        blocktree._cache.set_super_item(k, value)
        blocktree._read_queue.remove(k)
    except cloudnbd.QueueEmptyError:
      pass
  return reader

def _indep_get(blocktree, cloud, k):
  obj = cloud.get(k)
  if obj:
    data = obj.get_content()
    wire_data_len = len(data)
    data = blocktree._decrypt_data(k, data)
    with blocktree._stats_lock:
      blocktree._stats['recv_count'] += 1
      blocktree._stats['data_recv'] += len(data)
      blocktree._stats['wire_recv'] += wire_data_len
    cloud_checksum = obj.metadata['checksum']
    calc_checksum = blocktree._build_checksum(k, data)
    if cloud_checksum != calc_checksum:
      raise BTChecksumError(
       "remote and calculated checksums for object:%s don't match" % k
      )
    return data
  else:
    return None

class BlockTree(object):
  """Interface between cloud and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, cloud = None,
               threads = 1, read_ahead = 0):
    self._stats_lock = threading.RLock()
    self._stats = {'recv_count': 0, 'data_recv': 0, 'wire_recv': 0,
                   'sent_count': 0, 'data_sent': 0, 'wire_sent': 0,
                   'deleted_count': 0}
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.cloud = cloud
    self._cache = cloudnbd.Cache(backercb=self._cache_read_cb)
    # initialize the writer threads
    self._writers_active = False
    self.threads = threads
    # initialize the readahead threads
    self._readers_active = False
    self._read_ahead = read_ahead

  def start_writers(self):
    self._writers = []
    for i in xrange(self.threads):
      writer = threading.Thread(target=_writer_factory(self))
      writer.daemon = True
      self._writers.append(writer)
      writer.start()
    self._writers_active = True

  def start_readers(self):
    if self.read_ahead > 0:
      self._read_queue = cloudnbd.SyncQueue()
      self._readers = []
      for i in xrange(self.read_ahead):
        reader = threading.Thread(target=_reader_factory(self))
        reader.daemon = True
        self._readers.append(reader)
        reader.start()
      self._readers_active = True

  def get_stats(self):
    with self._stats_lock:
      comb_stats = dict(self._stats)
      comb_stats.update(self._cache.get_stats())
      return comb_stats

  def _cache_read_cb(self, k):
    return _indep_get(self, self.cloud, k)

  def set_cache_limits(self, total = None, write = None, flush = None):
    if total is not None: self._cache.total_size = total
    if write is not None: self._cache.queue_size = write
    if flush is not None: self._cache.flush_size = flush

  def set(self, path, data, direct = False):
    """Upload/queue an object on/to be uploaded to cloud."""
    if direct:
      checksum = self._build_checksum(path, data)
      cryptdata = self._encrypt_data(path, data)
      self.cloud.set(path, cryptdata, metadata={'checksum': checksum})
    else:
      self._cache[path] = data

  def _build_checksum(self, path, data):
    """Calculate the checksum for given path anda data."""
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.sha256(cloudnbd._salt + key
      + path.encode('utf8') + (b'' if data is None else data))
    return hasher.hexdigest()

  def _decrypt_data(self, path, data):
    """Decrypt the given data."""
    if not data:
      return None
    zipped, size = struct.unpack_from(b'!BQ', data, 0)
    data = data[struct.calcsize(b'!BQ'):]
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.md5(cloudnbd._salt + path.encode('utf8'))
    iv = hasher.digest()
    decryptor = AES.new(key, AES.MODE_CBC, iv)
    data = decryptor.decrypt(data)
    data = data[:size]
    magic_len = len(cloudnbd._crypt_magic)
    magic = data[-magic_len:]
    if magic != cloudnbd._crypt_magic:
      raise BTInvalidKey("decryption of '%s' failed possibly due to"
                         " invalid encryption key (or passphrase)"
                         % path)
    data = data[:-magic_len]
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
    data += cloudnbd._crypt_magic
    header = struct.pack(b'!BQ', storezip, len(data))
    data = data.ljust((len(data) // 32 + 1) * 32, b'\0')
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.md5(cloudnbd._salt + path.encode('utf8'))
    iv = hasher.digest()
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    data = encryptor.encrypt(data)
    return header + data

  def get(self, path):
    """Get the value of an object."""
    if self._readers_active:
      m = re.match(r'^(.*?blocks/)(\d+)$', path)
      if m:
        s = int(m.group(2)) + 1
        e = s + self._read_ahead + 1
        for b in xrange(s, e):
          ra_k = '%s%d' % (m.group(1), b)
          if ra_k not in self._cache:
            self._read_queue.push(ra_k)
    return self._cache[path]

  def close(self):
    if self._writers_active:
      self._cache.set_wait_on_empty(False)
      for th in self._writers:
        th.join()
