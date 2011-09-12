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
import shelve
import glob
import time
from Crypto.Cipher import AES

class BTError(Exception):
  pass
class BTDecryptFailed(BTError):
  pass
class BTChecksumError(BTError):
  pass

class BlockTree(object):
  """Interface between S3/Local cache and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, s3 = None):
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.s3 = s3
    self._transaction = {}
    self._checksums = {}
    self.trans_size = 0

  def set(self, path, data, direct = False, dont_cache = False):
    """Set the value of an object locally and optionally remotely. If
    direct is set to True, the data is immediately uploaded to S3.
    """
    checksum = self._build_checksum(path, data)
    cryptdata = self._encrypt_data(path, data)
    if not (direct and dont_cache):
      self._set_local(path, cryptdata)
      self._set_checksum(path, checksum)
    if direct:
      self.s3.set(path, cryptdata, metadata={'checksum': checksum})
    else:
      if path not in self._transaction:
        self.trans_size += len(data)
      self._transaction[path] = self._checksums[path][0]

  def _build_checksum(self, path, data):
    """Calculate the checksum for given path anda data."""
    key = self.pass_key if path == 'config' else self.crypt_key
    hasher = hashlib.sha256(s3nbd._salt + key
      + path.encode('utf8') + (b'' if data is None else data))
    return hasher.hexdigest()

  def _download(self, path, store_locally = True):
    """Download the object from S3 and save it locally and return the
    plain value.
    """
    remoteobj = self.s3.get(path)
    cryptdata = remoteobj.get_content() if remoteobj else None
    try:
      plaindata = self._decrypt_data(path, cryptdata)
    except:
      raise BTDecryptFailed('Decryption of remote obj failed due to'
                            ' incorrect key/passphrase or corruption')
    checksum = self._build_checksum(path, plaindata)
    if remoteobj and checksum != remoteobj.metadata['checksum']:
      raise BTChecksumError("Remote object's data checksum does not"
                            " match its metadata checksum")
    if store_locally:
      if remoteobj:
        self._set_local(path, cryptdata)
      self._set_checksum(path, checksum)
    return plaindata

  def _get_local_path(self, path):
    """Get the physical path for the given relative path of object."""
    return os.path.join(
      s3nbd._local_cache_dir,
      'objects',
      self.s3.bucket,
      self.s3.volume,
      path
    )

  def _get_local(self, path):
    """Return the content of the object from local cache."""
    filepath = self._get_local_path(path)
    if os.path.exists(filepath):
      return open(filepath, 'rb').read()
    else:
      return None

  def _set_local(self, path, data):
    """Set the content of the object in the local cache."""
    filepath = self._get_local_path(path)
    if not data:
      if os.path.exists(filepath):
        os.unlink(filepath)
      return
    dirname = os.path.dirname(filepath)
    if not os.path.exists(dirname):
      os.makedirs(dirname)
    open(filepath, 'wb').write(data)

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

  def get(self, path, cache = 'use'):
    """Get the value of an object.
    
    Parameters:
    cache - if set to 'ignore', all local cache operations are bypassed,
            if set to 'store_only', local cache is not asked for cached
            data but the new downloaded data will be stored in it, and
            the default value 'use' makes use of cache for both
            purpsoses
    """
    if not cache == 'use':
      return self._download(path,
        store_locally=(cache != 'ignore'))
    try:
      data = self._decrypt_data(path, self._get_local(path))
    except:
      data = None
      checksum = None
      local_checksum = None
      self._del_checksum(path)
    else:
      checksum = self._build_checksum(path, data)
      local_checksum = self._get_checksum(path)
    if path in self._transaction:
      if local_checksum is None:
        raise BTChecksumError('Missing local checksum of an'
                              ' in-transaction object')
      if local_checksum != checksum:
        raise BTChecksumError('Mismatching local checksum of an'
                              ' in-transaction obj')
      return data
    if local_checksum is None:
      remoteobj = self.s3.get(path)
      if remoteobj:
        remote_checksum = remoteobj.metadata['checksum']
      else:
        remote_checksum = self._build_checksum(path, None)
      local_checksum = remote_checksum
      self._set_checksum(path, remote_checksum)
    if checksum != local_checksum:
      return self._download(path)
    else:
      return data

  def _set_checksum(self, path, checksum):
    self._checksums[path] = (checksum, time.time())
    if len(self._checksums) > s3nbd._checksum_cache_size:
      new_size = int(s3nbd._checksum_cache_size *
                     s3nbd._checksum_cache_reduction_ratio)
      key_list = self._checksums.items()
      key_list.sort(cmp=lambda a, b: cmp(a[1][1], b[1][1]),
                    reverse=True)
      self._checksums = dict(key_list[:new_size])

  def _del_checksum(self, path):
    if path in self._checksums:
      del self._checksums[path]

  def _get_checksum(self, path):
    if path in self._transaction:
      return self._transaction[path]
    else:
      return self._checksums[path][0] if path in self._checksums \
        else None

  def commit(self):
    """Commit the outstanding transaction."""
    if not self._transaction:
      return
    tran_log = s3nbd.serialize_config(self._transaction.keys())
    self.set('trans', tran_log, direct=True, dont_cache=True)
    for path in self._transaction:
      checksum = self._transaction[path]
      cryptdata = self._get_local(path)
      try:
        plaindata = self._decrypt_data(path, cryptdata)
      except:
        raise BTDecryptFailed('In-transaction object failed to decrypt'
                              ' due to possible local file system'
                              ' corruption or tampering')
      local_checksum = self._build_checksum(path, plaindata)
      if checksum != local_checksum:
        raise BTChecksumError('Local stored checksum is different to'
                              ' actual checksum of in-transaction'
                              ' object')
      s3_path = 'trans/' + path
      self.s3.set(s3_path, cryptdata, metadata={'checksum': checksum})
    self.finalize_transaction(transaction_objects=self._transaction,
      committed_objects=self._transaction)
    self._transaction = {}
    self.trans_size = 0

  def finalize_transaction(self, transaction_objects = None,
                           committed_objects=None):
    """Move data from temporary area on S3 to permanent place."""
    if transaction_objects is None:
      tran_log = self.get('trans', cache='ignore')
      if not tran_log:
        return
      transaction_objects = s3nbd.deserialize_config(tran_log)
      
    if not transaction_objects:
      return

    if committed_objects is None:
      committed_objects = set()
      for path in transaction_objects:
        remoteobj = self.s3.get('trans/' + path)
        if remoteobj:
          committed_objects.add(path)

    if len(committed_objects) == len(transaction_objects):
      for path in committed_objects:
        self.s3.copy('trans/' + path, path)
    for path in committed_objects:
      self.s3.delete('trans/' + path)
    self.s3.delete('trans')

