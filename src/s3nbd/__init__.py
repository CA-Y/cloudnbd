"""
TODO
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
import json
import time

_ver_major = 0
_ver_minor = 1
_ver_patch = 0
_ver_sub = ''
_ver_tuple = (_ver_major, _ver_minor, _ver_patch, _ver_sub)

__version__ = '%d.%d.%d%s' % _ver_tuple

_prog_name = 's3bd'
_print_ver = '%s %s' % (_prog_name, __version__)
_local_cache_dir = '/var/cache/s3bd'
_local_run_dir = '/var/run/s3bd'

_default_bs = 2 ** 16
_default_bmp_bs = 2 ** 10
_default_refcnt_bs = 2 ** 10
_default_root = 'cur'
_default_bind = ''
_default_port = 7323
_checksum_cache_size = 10000 # entries
_checksum_cache_reduction_ratio = 0.7
_block_cache_size = 20 # entries
_block_cache_reduction_ratio = 0.7
_refcnt_cache_size = _default_refcnt_bs * (2 ** 7) # entries
_refcnt_cache_reduction_ratio = 0.7
_bmp_cache_size = _default_bmp_bs * (2 ** 7) # entries
_bmp_cache_reduction_ratio = 0.7
_max_commit_size = 2 ** 23

_salt = b'EyAEAPVOvfqERT8hsJB5tgy0dB0x7Erp'

class SerializeFailed(Exception):
  pass

def deserialize(data):
  try:
    return json.loads(data)
  except ValueError:
    raise SerializeFailed('Failed to decode the data')

def serialize(data):
  try:
    return json.dumps(data)
  except TypeError:
    raise SerializeFailed('Failed to encode the data')

class CacheDict(dict):

  def __init__(self, *args, **kargs):
    super(CacheDict, self).__init__(*args, **kargs)
    def _def_backer(obj, key):
      pass
    self.backercb = \
      kargs['backercb'] if 'backercb' in kargs else _def_backer
    self.max_entries = \
      kargs['max_entries'] if 'max_entries' in kargs else None
    self.drop_ratio = \
      kargs['drop_ratio'] if 'drop_ratio' in kargs else 0.75
    self.mask_dict = \
      kargs['mask_dict'] if 'mask_dict' in kargs else None
    self._ts = {}

  def __getitem__(self, key):
    if self.mask_dict is not None:
      try:
        return self.mask_dict[key]
      except KeyError:
        pass
    try:
      return super(CacheDict, self).__getitem__(key)
    except KeyError:
      self.backercb(self, key)
      return super(CacheDict, self).__getitem__(key)

  def __setitem__(self, key, value):
    super(CacheDict, self).__setitem__(key, value)
    self._ts[key] = time.time()
    if self.max_entries and len(self) > self.max_entries:
      new_size = int(self.max_entries * self.drop_ratio)
      tss = self._ts.items()
      tss.sort(cmp=lambda a, b: cmp(a[1], b[1]), reverse=True)
      to_delete = tss[new_size:]
      for k, t in to_delete:
        del self._ts[k]
        del self[k]

from s3nbd import cmd
from s3nbd import auth
from s3nbd import s3
from s3nbd import blocktree
from s3nbd import nbd
