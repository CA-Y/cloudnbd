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
import cnbdcore
import os
import struct
import hashlib
import time
import threading
import re
from Crypto.Cipher import AES

class BTError(Exception):
  pass
class BTDecryptError(BTError):
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
          plain_data_len = len(data)
          data = blocktree._encrypt_data(path, data)
          cloud.set(path, data)
          with blocktree._stats_lock:
            blocktree._stats['sent_count'] += 1
            blocktree._stats['data_sent'] += plain_data_len
            blocktree._stats['wire_sent'] += len(data)
        blocktree._cache.unpin(path)
        del data
    except cnbdcore.QueueEmptyError:
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
                   #     it's ok to skip over checksum issues
        blocktree._cache.set_super_item(k, value)
        blocktree._read_queue.remove(k)
    except cnbdcore.QueueEmptyError:
      pass
  return reader

def _indep_get(blocktree, cloud, k):
  obj = cloud.get(k)
  with blocktree._stats_lock:
    blocktree._stats['recv_count'] += 1
  if obj:
    data = obj.get_content()
    wire_data_len = len(data)
    data = blocktree._decrypt_data(k, data)
    with blocktree._stats_lock:
      blocktree._stats['data_recv'] += len(data)
      blocktree._stats['wire_recv'] += wire_data_len
    return data
  else:
    return None

class BlockTree(object):
  """Interface between cloud and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, cloud = None,
               threads = 1, read_ahead = 0, compressor = None):
    self._stats_lock = threading.RLock()
    self._stats = {'recv_count': 0, 'data_recv': 0, 'wire_recv': 0,
                   'sent_count': 0, 'data_sent': 0, 'wire_sent': 0,
                   'deleted_count': 0}
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.cloud = cloud
    self._cache = cnbdcore.Cache(backercb=self._cache_read_cb)
    # initialize the writer threads
    self._writers_active = False
    self.threads = threads
    # initialize the readahead threads
    self._readers_active = False
    self._read_ahead = read_ahead
    self.compressor = compressor

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
      self._read_queue = cnbdcore.SyncQueue()
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
      cryptdata = self._encrypt_data(path, data)
      self.cloud.set(path, cryptdata)
    else:
      self._cache[path] = data

  _cs_len = 256 // 8
  def _build_checksum(self, path, data):
    """Calculate the checksum for given path and data."""
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.sha256(cnbdcore._salt + key
      + path.encode('utf8') + (b'' if data is None else data))
    return hasher.digest()

  def _decrypt_data(self, path, data):
    """Decrypt the given data."""
    if not data:
      return None

    # decrypt the data

    if len(data) % BlockTree._crypt_bl != 0:
      raise BTDecryptError("decryption of '%s' failed due to"
                           " possible corruption" % path)
    hasher = hashlib.md5(cnbdcore._salt + path.encode('utf8'))
    key = self.pass_key if path == 'config' else self.crypt_key
    iv = hasher.digest()
    decryptor = AES.new(key, AES.MODE_CBC, iv)
    data = decryptor.decrypt(data)

    # check the magic string to ensure correct decryption

    if data[-(len(cnbdcore._crypt_magic)):] != cnbdcore._crypt_magic:
      raise BTDecryptError("decryption of '%s' failed possibly due to"
                           " invalid encryption key (or passphrase)"
                           % path)

    # decode the header

    header_spec = b'!%dsBQ' % BlockTree._cs_len
    header_len = struct.calcsize(header_spec)
    checksum, is_com, dl = struct.unpack_from(header_spec, data, 0)
    data = data[header_len:header_len + dl]

    # decompress if needed

    if is_com:
      data = self.compressor.decompress(data)

    # compare the stored checksum with the data's checksum

    if checksum != self._build_checksum(path, data):
      raise BTChecksumError(
       "remote and calculated checksums for object:%s don't match"
       % path
      )

    # all good

    return data

  _crypt_bl = 32
  def _encrypt_data(self, path, data):
    """Encrypt the given data."""
    if not data:
      return None

    # get the checksum

    checksum = self._build_checksum(path, data)

    # decide whether to compress the data

    if path == 'config':
      store_compressed = 0
    else:
      compressed = self.compressor.compress(data)
      if len(compressed) < len(data):
        store_compressed = 1
        data = compressed
      else:
        store_compressed = 0
        del compressed
    data_len = len(data)

    # build the packet

    header_spec = b'!%dsBQ' % BlockTree._cs_len
    header = struct.pack(
      header_spec, checksum, store_compressed, data_len
    )

    # calculate the pad length

    hd_len = len(header) + len(data) + len(cnbdcore._crypt_magic)
    pad_len = (hd_len // BlockTree._crypt_bl + 1) \
      * BlockTree._crypt_bl - hd_len

    # construct the packet

    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.md5(cnbdcore._salt + path.encode('utf8'))
    iv = hasher.digest()
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    return encryptor.encrypt(
      header + data + (b'\0' * pad_len) + cnbdcore._crypt_magic
    )

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

  def flush(self):
    lock = threading.RLock()
    wait_obj = threading.Condition(lock)
    with lock:
      if self._cache.flush_dirty(wait_obj):
        wait_obj.wait()

  def close(self):
    if self._writers_active:
      self._cache.set_wait_on_empty(False)
      for th in self._writers:
        th.join()
