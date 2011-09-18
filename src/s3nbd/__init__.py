"""
TODO
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
import json
import time
import threading

_ver_major = 0
_ver_minor = 1
_ver_patch = 0
_ver_sub = ''
_ver_tuple = (_ver_major, _ver_minor, _ver_patch, _ver_sub)

__version__ = '%d.%d.%d%s' % _ver_tuple

_prog_name = 's3bd'
_print_ver = '%s %s' % (_prog_name, __version__)

_default_bs = 2 ** 16
_default_bind = ''
_default_port = 7323
_default_write_cache_size = 2 ** 23
_default_total_cache_size = 2 ** 24
_default_write_thread_count = 10

_salt = b'\xbe\xee\x0f\xac\x81\xb9x7n\xce\xd6\xd0\xdfc\xc8\x11\x91+' \
        b'\x9d2&\xe5\x14<O\x0b\xabyF[\xea\xdcA\xc8\\\x8c\xaez&\xf8' \
        b'\xb9H\xcc\xe4\xf5\x9bs\xc0\xba\xab\xf0\x1b\xb4\xdb\xf6T' \
        b'\xe9\xe2\xc1\xc3R]\xc0\xd1'

class SerializeFailed(Exception):
  pass

def deserialize(data):
  try:
    return json.loads(data)
  except ValueError:
    raise SerializeFailed('Deserialization failed')

def serialize(data):
  try:
    return json.dumps(data)
  except TypeError:
    raise SerializeFailed('Serialization failed')

class QueueEmptyError(Exception):
  pass

class Cache(dict):

  def __init__(self, *args, **kargs):
    super(Cache, self).__init__(*args, **kargs)
    def _def_backer(key):
      return None
    self.backercb = \
      kargs['backercb'] if 'backercb' in kargs else _def_backer
    self.queue_size = \
      kargs['queue_size'] if 'queue_size' in kargs else None
    self.total_size = \
      kargs['total_size'] if 'total_size' in kargs else self.queue_size
    self._ts = {}
    self._queue = []
    self._lock = threading.RLock()
    self._set_wait = threading.Condition(self._lock)
    self._dequeue_wait = threading.Condition(self._lock)
    self._wait_on_empty = True

  def __getitem__(self, key):
    try:
      with self._lock:
        return super(Cache, self).__getitem__(key)
    except KeyError:
      value = self.backercb(key)
      with self._lock:
        while len(self._queue) == self.total_size:
          self._set_wait.wait()
        super(Cache, self).__setitem__(key, value)
        self._ts[key] = time.time()
        self._trim()
      return value

  def _trim(self):
    """Trim the unqueued items down to the total size."""
    with self._lock:
      if len(self) > self.total_size:
        unqueued = filter(lambda a: a not in self._queue, self.keys())
        unqueued.sort(cmp=lambda a, b: cmp(self._ts[a], self._ts[b]))
        unqueued = unqueued[0:len(self) - self.total_size]
        for k in unqueued:
          del self._ts[k]
          del self[k]

  def __setitem__(self, key, value):
    with self._lock:
      while (key not in self._queue
             and len(self._queue) == self.queue_size):
        self._set_wait.wait()
      super(Cache, self).__setitem__(key, value)
      self._ts[key] = time.time()
      if key in self._queue:
        self._queue.remove(key)
      self._queue.append(key)
      self._trim()
      self._dequeue_wait.notify()

  def dequeue(self):
    with self._lock:
      while not self._queue and self._wait_on_empty:
        self._dequeue_wait.wait()
      if not self._queue and not self._wait_on_empty:
        raise QueueEmptyError('No item in the queue')
      key = self._queue.pop(0)
      self._set_wait.notify()
      return (key, super(Cache, self).__getitem__(key))

  def set_wait_on_empty(self, v):
    with self._lock:
      self._wait_on_empty = v
      self._dequeue_wait.notify_all()

from s3nbd import cmd
from s3nbd import auth
from s3nbd import s3
from s3nbd import blocktree
from s3nbd import nbd
