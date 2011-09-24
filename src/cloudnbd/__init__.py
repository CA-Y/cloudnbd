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

_prog_name = 'cloudbd'
_print_ver = '%s %s' % (_prog_name, __version__)

_default_bs = 2 ** 16
_default_bind = ''
_default_port = 7323
_default_total_cache_size = 2 ** 24
_write_to_total_cache_ratio = 0.5
_write_queue_to_flush_ratio = 0.7
_default_write_thread_count = 10
_default_read_ahead_count = 3
_stat_path = '/tmp/' + _prog_name + '-%s-%s-%s.stat'
_pid_path = '/tmp/' + _prog_name + '-%s-%s-%s.pid'

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

class SyncQueue(object):
  def __init__(self):
    self._queue = []
    self._items = set()
    self._lock = threading.RLock()
    self._wait = threading.Condition(self._lock)

  def push(self, v):
    with self._lock:
      if v not in self._items:
        self._queue.append(v)
        self._wait.notify()

  def pop(self):
    with self._lock:
      while not self._queue:
        self._wait.wait()
      v = self._queue.pop(0)
      return v

  def remove(self, k):
    with self._lock:
      if k in self._items:
        self._items.remove(k)
      if k in self._queue:
        self._queue.remove(k)

def _def_backer(key):
  return None

class Cache(dict):

  def __init__(self, backercb = _def_backer):
    super(Cache, self).__init__()
    self._backercb = backercb
    self.total_size = 1
    self.queue_size = 1
    self.flush_size = 1
    self._ts = {}
    self._pinned = set()
    self._queue = []
    self._lock = threading.RLock()
    self._set_wait = threading.Condition(self._lock)
    self._dequeue_wait = threading.Condition(self._lock)
    self._wait_on_empty = True

  def __contains__(self, key):
    with self._lock:
      return super(Cache, self).__contains__(key)

  def __getitem__(self, key):
    try:
      with self._lock:
        return super(Cache, self).__getitem__(key)
    except KeyError:
      value = self._backercb(key)
      return self.set_super_item(key, value)

  def set_super_item(self, key, value):
    with self._lock:
      if not super(Cache, self).__contains__(key):
        super(Cache, self).__setitem__(key, value)
        self._ts[key] = time.time()
        self._trim()
        return value
      else:
        return super(Cache, self).__getitem__(key)

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
      if len(self._queue) == self.queue_size:
        self._dequeue_wait.notify_all()

  def _pop_next_unpinned_key(self):
    for i, key in zip(xrange(len(self._queue)), self._queue):
      if key not in self._pinned:
        self._queue.pop(i)
        self._pinned.add(key)
        self._set_wait.notify_all()
        return key
    return None

  def dequeue(self):
    with self._lock:
      while True:
        if self._wait_on_empty:
          if len(self._queue) < self.flush_size:
            self._dequeue_wait.wait()
            continue
          else:
            key = self._pop_next_unpinned_key()
            if key is None:
              self._dequeue_wait.wait()
              continue
            break
        else:
          if self._queue:
            key = self._pop_next_unpinned_key()
            if key is None:
              self._dequeue_wait.wait()
              continue
            break
          else:
            raise QueueEmptyError('No item in the queue')
      return (key, super(Cache, self).__getitem__(key))

  def unpin(self, key):
    with self._lock:
      if key in self._pinned:
        self._pinned.remove(key)
        self._dequeue_wait.notify_all()

  def set_wait_on_empty(self, v):
    with self._lock:
      self._wait_on_empty = v
      self._dequeue_wait.notify_all()

from cloudnbd import cmd
from cloudnbd import auth
from cloudnbd import cloud
from cloudnbd import blocktree
from cloudnbd import nbd
