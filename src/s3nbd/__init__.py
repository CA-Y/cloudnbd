"""
TODO
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
import json

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

_salt = b'EyAEAPVOvfqERT8hsJB5tgy0dB0x7Erp'

class InvalidConfigFormat(Exception):
  pass

def deserialize_config(data):
  try:
    return json.loads(data)
  except ValueError:
    raise InvaliConfigFormat('Failed to decode the config data')

def serialize_config(data):
  try:
    return json.dumps(data)
  except TypeError:
    raise InvaliConfigFormat('Failed to encode the config data')

from s3nbd import cmd
from s3nbd import auth
from s3nbd import s3
from s3nbd import blocktree
