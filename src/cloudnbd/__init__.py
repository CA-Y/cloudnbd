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
import re
import glob
import fcntl
import os
import stat

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
_default_delete_thread_count = 30
_default_read_ahead_count = 3
_stat_path = '/tmp/' + _prog_name + ':%s:%s:%s:%s'
_stat_pat = re.compile(
  r'/' + _prog_name + r':([^:]+):([^:]+):([^:]+):pid$'
)
_open_volumes_glob = '/tmp/' + _prog_name + ':*:*:*:pid'

_salt = b'\xbe\xee\x0f\xac\x81\xb9x7n\xce\xd6\xd0\xdfc\xc8\x11\x91+' \
        b'\x9d2&\xe5\x14<O\x0b\xabyF[\xea\xdcA\xc8\\\x8c\xaez&\xf8' \
        b'\xb9H\xcc\xe4\xf5\x9bs\xc0\xba\xab\xf0\x1b\xb4\xdb\xf6T' \
        b'\xe9\xe2\xc1\xc3R]\xc0\xd1'
_crypt_magic = b'C10Ud-LiC1ou5'

_locked_pids = {}

def size_to_hum(size):
  if size < 1100:
    return '%d B' % size
  elif size < 1100000:
    return '%.1f KB' % (size / 1000)
  elif size < 1100000000:
    return '%.1f MB' % (size / 1000000)
  elif size < 1100000000000:
    return '%.1f GB' % (size / 1000000000)
  elif size < 1100000000000000:
    return '%.1f TB' % (size / 1000000000000)
  else:
    return '%.1f PB' % (size / 1000000000000000)

def acquire_pid_lock(backend, bucket, volume):
  """Attempt to acquire pid lock. Return True if successfully acquired,
  False otherwise.
  """
  fp = open(get_pid_path(backend, bucket, volume), 'w')
  try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
  except IOError:
    return False
  _locked_pids[(backend, bucket, volume)] = fp
  return True

def release_pid_lock(backend, bucket, volume):
  """Release an already acquired pid lock. Return True if successfully
  release, False otherwise.
  """
  k = (backend, bucket, volume)
  if k in _locked_pids:
    os.unlink(get_pid_path(backend, bucket, volume))
    _locked_pids[k].close()
    del _locked_pids[k]
    return True
  return False

def release_all_pid_locks():
  for backend, bucket, volume in list(_locked_pids.keys()):
    release_pid_lock(backend, bucket, volume)

def create_stat_node(backend, bucket, volume):
  destroy_stat_node(backend, bucket, volume)
  p = get_stat_path(backend, bucket, volume)
  try:
    os.mknod(p, 0644 | stat.S_IFIFO)
  except:
    return False
  return True

def destroy_stat_node(backend, bucket, volume):
  try:
    os.unlink(get_stat_path(backend, bucket, volume))
  except:
    return False
  return True

def get_stat_path(backend, bucket, volume):
  return _stat_path % (backend, bucket, volume, 'stat')

def get_pid_path(backend, bucket, volume):
  return _stat_path % (backend, bucket, volume, 'pid')

def get_open_volumes_list():
  file_list = glob.glob(_open_volumes_glob)
  file_list = map(get_vol_id_for_path, file_list)
  return filter(lambda a: a is not None, file_list)

def get_vol_id_for_path(path):
  m = _stat_pat.search(path)
  if m:
    return (m.group(1), m.group(2), m.group(3))
  return None

class Interrupted(Exception):
  pass

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
        self._items.add(v)
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
    self._greedy_dequeue = False

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

  def flush_dirty(self, wait_obj):
    """Drain the queue and notify the wait_obj

    Return: True if waiting is necessary, False if the queue is already
            empty.
    """
    with self._lock:
      if not self._queue and not self._pinned:
        return False
      else:
        self._greedy_dequeue = True
        self._greedy_dequeue_wait_obj = wait_obj
        self._dequeue_wait.notify_all()
        return True

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
          if (not self._greedy_dequeue
              and len(self._queue) < self.flush_size) \
             or (self._greedy_dequeue and not self._queue):
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
      if self._greedy_dequeue and not self._pinned and not self._queue:
        self._greedy_dequeue = False
        self._greedy_dequeue_wait_obj.acquire()
        self._greedy_dequeue_wait_obj.notify_all()
        self._greedy_dequeue_wait_obj.release()
        self._greedy_dequeue_wait_obj = None

  def set_wait_on_empty(self, v):
    with self._lock:
      self._wait_on_empty = v
      self._dequeue_wait.notify_all()

  def get_stats(self):
    with self._lock:
      return {
        'cache_size': super(Cache, self).__len__(),
        'queue_size': len(self._queue)
      }

from cloudnbd import cmd
from cloudnbd import auth
from cloudnbd import cloud
from cloudnbd import blocktree
from cloudnbd import nbd
from cloudnbd import daemon
