#!/usr/bin/env python
#
# s3bd.py - NBD server with Amazon S3 cloud storage system as the
#           backend
# Copyright (C) Mansour <mansour@oxplot.com>
#

import sys
import re
import socket
import struct
import threading
import time
import logging as log
import zlib

log.basicConfig(
  format='%(filename)s:%(funcName)s:%(lineno)d:%(message)s',
  level=log.INFO
)

def err(msg):
  print 's3bd: ' + msg

def fatal(msg):
  err(msg)
  exit(2)

class NBD:

  READ = 0
  WRITE = 1
  CLOSE = 2

  def __init__(self, host = '', port = 12345, size = 0):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind((host, port))
    self.size = size
    self.readcb = None
    self.writecb = None
    self.closecb = None
    self.closed = False
    self.opened = False

  def run(self):
    self.sock.listen(1)
    self.sock, addr = self.sock.accept()
    self.opened = True
    self.sock.send('NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53' +
      struct.pack('>Q', self.size) + '\0' * 128)
    while True:
      header = self.receive(struct.calcsize('>LL8sQL'))
      mag, request, han, off, dlen = struct.unpack('>LL8sQL', header)
      assert mag == 0x25609513
      if request == NBD.READ:
        self.sock.send('gDf\x98\0\0\0\0' + han)
        v = self.readcb(off, dlen)
        self.sock.send(self.readcb(off, dlen))
      elif request == NBD.WRITE:
        self.writecb(off, self.receive(dlen))
        self.sock.send('gDf\x98\0\0\0\0' + han)
      elif request == NBD.CLOSE:
        self.sock.close()
        self.closed = True
        self.closecb()
        return

  def receive(self, length):
    rv = []
    while sum(map(len, rv)) < length:
      rv.append(self.sock.recv(length - sum(map(len, rv))))
      assert rv[-1], "no more data to read"
    return ''.join(rv)

class S3:
  def __init__(self, accesskey, secretkey, bucket):
    from boto.s3.connection import S3Connection
    self.conn = S3Connection(accesskey, secretkey)
    self.bucket = self.conn.get_bucket(bucket)
    self.oplock = threading.RLock()
  def get(self, key):
    key = self.bucket.get_key(key)
    if key:
      return key.get_contents_as_string()
    else:
      return None
  def set(self, key, value):
    from boto.s3.connection import Key
    with self.oplock:
      k = Key(self.bucket)
      k.key = key
      k.set_contents_from_string(value)
  def exists(self, key):
    with self.oplock:
      return True if self.bucket.get_key(key) else False
  def list(self, prefix=''):
    return self.bucket.list(prefix=prefix)
  def delete(self, key):
    from boto.s3.connection import Key
    with self.oplock:
      k = Key(self.bucket)
      k.key = key
      k.delete()

class Block:
  def __init__(self, data, timestamp):
    self.data = data
    self.timestamp = timestamp

class Options:
  def __init__(self):
    self.forceclose = False
    self.debug = False
    self.blocksize = 1024 * 500
    self.totalsize = None
    self.bucket = None
    self.prefix = None
    self.accesskey = None
    self.secretkey = None
    self.action = None
    self.cacheupper = 20
    self.cachelower = 15
    self.ip = None
    self.port = None

class S3BD:

  def __init__(self):
    self.opts = Options()
    self.usage = """Usage: s3bd.py <action> <... args ...> [options]
  Creating a new block storage:
    s3bd.py create <accesskey> <secretkey> <bucket> <prefix> <size>
                   [<blocksize>]
  Resize a storage:
    s3bd.py resize <accesskey> <secretkey> <bucket> <prefix> <size>
  Garbage collect:
    s3bd.py gc <accesskey> <secretkey> <bucket> <prefix>
  Connecting to an existing storage:
    s3bd.py open <accesskey> <secretkey> <bucket> <prefix>
                 <[bind-ip]:port>
  Disconnecting from a connected storage:
    s3bd.py close <bucket> <prefix>
  Disconnecting from all currently connected storages:
    s3bd.py closeall
  Options:
    -D  Turns the debugging mode on
    -F  Force close the connection despite dirty cache
"""


  def optserr(self, msg):
    err(msg)
    print self.usage
    exit(1)

  def parseopts(self):
    args = sys.argv[1:]
    self.opts.debug = '-D' in args
    self.opts.forceclose = '-F' in args
    args = [i for i in args if not i.startswith('-')]
    if len(args) < 1:
      self.optserr('no action specified')
    self.opts.action = args[0]
    if self.opts.action in ('create', 'open', 'resize', 'gc'):
      if len(args) < 5:
        self.optserr('not enough arguments')
      self.opts.accesskey = args[1]
      self.opts.secretkey = args[2]
      self.opts.bucket = args[3]
      self.opts.prefix = args[4]
      if self.opts.action in ('create', 'resize'):
        if len(args) < 6:
          self.optserr('not enough arguments')
        self.opts.totalsize = int(args[5])
        if len(args) > 6:
          self.blocksize = int(args[6])
      elif self.opts.action == 'open':
        if len(args) < 6:
          self.optserr('not enough arguments')
        self.opts.ip, self.opts.port = args[5].split(':')
        self.opts.port = int(self.opts.port)
    elif self.opts.action == 'close':
      if len(args) < 3:
        self.optserr('not enough arguments')
      self.opts.bucket = args[1]
      self.opts.prefix = args[2]
    elif self.opts.action == 'closeall':
      pass
    else:
      self.optserr('invalid action')

  def run(self):
    self.parseopts()
    if self.opts.debug:
      pass # TODO do SOMETHING !!
    {'create': self.create,
     'open': self.open,
     'close': self.close,
     'closeall': self.closeall,
     'resize': self.resize,
     'gc': self.gc
    }[self.opts.action]()

  def inits3(self):
    self.s3 = S3(self.opts.accesskey,
                 self.opts.secretkey,
                 self.opts.bucket)

  def open(self):
    self.inits3()
    meta = self.s3.get('%s/info' % self.opts.prefix)
    if meta is None:
      fatal("'%s' storage not found in '%s' bucket,"
            " use 's3bd.py create' to create one"
            % (self.opts.prefix, self.opts.bucket))
    self.opts.totalsize, self.opts.blocksize = map(int, meta.split(','))
    self.emptyblock = '\0' * self.opts.blocksize

    self.nbd = NBD(size=self.opts.totalsize)
    self.nbd.readcb = self.read
    self.nbd.writecb = self.write
    self.nbd.closecb = self.flushall

    self.cache = {}
    self.cachelock = threading.RLock()
    self.dirty = set()

    self.workerstop = False
    self.workerthread = threading.Thread()
    self.workerthread.run = self.worker
    self.workerthread.daemon = True # FIXME delete this line
    self.workerthread.start()
    self.nbd.run()
    self.workerstop = True
    self.workerthread.join()

  def create(self):
    self.inits3()
    if self.s3.exists('%s/info' % self.opts.prefix):
      fatal("'%s' storage in '%s' bucket is an existing storage"
            % (self.opts.prefix, self.opts.bucket))
    meta = '%d,%d' % (self.opts.totalsize, self.opts.blocksize)
    self.s3.set('%s/info' % self.opts.prefix, meta)

  def resize(self):
    self.inits3()
    meta = self.s3.get('%s/info' % self.opts.prefix)
    if meta is None:
      fatal("'%s' storage not found in '%s' bucket,"
            " use 's3bd.py create' to create one"
            % (self.opts.prefix, self.opts.bucket))
    size, bs = map(int, meta.split(','))
    meta = '%d,%d' % (self.opts.totalsize, bs)
    self.s3.set('%s/info' % self.opts.prefix, meta)

  def gc(self):
    self.inits3()
    meta = self.s3.get('%s/info' % self.opts.prefix)
    if meta is None:
      fatal("'%s' storage not found in '%s' bucket,"
            " use 's3bd.py create' to create one"
            % (self.opts.prefix, self.opts.bucket))
    size, bs = map(int, meta.split(','))
    maxid = size / bs
    delmark = []
    for i in self.s3.list('%s/block-' % self.opts.prefix):
      id = int(re.findall(r'-(\d+)$', i.key)[0])
      if id > maxid:
        delmark.append(id)
    for i, b in enumerate(delmark):
      self.s3.delete('%s/block-%d' % (self.opts.prefix, b))
      print '%d of %d blocks deleted ...' % (i, len(delmark))

  def close(self):
    pass

  def closeall(self):
    pass

  def read(self, off, length):
    bs = self.opts.blocksize
    block = off / bs
    start = off % bs
    end = (min(off + length, (block + 1) * bs) - 1) % bs + 1
    data = []
    while block * bs < off + length:
      data.append(self.getblock(block)[start:end])
      start = 0
      end = (min(off + length, (block + 2) * bs) - 1) % bs + 1
      block += 1
    return ''.join(data)

  def write(self, off, data):
    length = len(data)
    datap = 0
    bs = self.opts.blocksize
    block = off / bs
    start = off % bs
    end = (min(off + length, (block + 1) * bs) - 1) % bs + 1
    while block * bs < off + length:
      if end - start < bs:
        bd = self.getblock(block)
        bd = bd[:start] + data[datap:end - start + datap] \
          + bd[end:]
        self.setblock(block, bd)
      else:
        self.setblock(block, data[datap:end - start + datap])
      datap += end - start
      start = 0
      end = (min(off + length, (block + 2) * bs) - 1) % bs + 1
      block += 1

  def getblock(self, bid):
    with self.cachelock:
      if bid in self.cache:
        self.cache[bid].timestamp = time.time()
        return self.cache[bid].data
    bd = self.s3.get("%s/block-%d" % (self.opts.prefix, bid))
    if bd is None:
      bd = self.emptyblock
    else:
      bd = zlib.decompress(bd)
    with self.cachelock:
      if bid not in self.cache:
        self.cache[bid] = Block(bd, time.time())
        self.cachecontrol()
    return bd

  def setblock(self, bid, data):
    with self.cachelock:
      self.cache[bid] = Block(data, time.time())
      self.dirty.add(bid)
      self.cachecontrol()

  def cachecontrol(self):
    if len(self.cache) > self.opts.cacheupper:
      while len(self.dirty) > 0:
        self.cachelock.release()
        time.sleep(0.1)
        self.cachelock.acquire()
      sortedcache = [(k, v) for k, v in self.cache.iteritems()]
      sortedcache.sort(
        cmp=(lambda a, b: cmp(a[1].timestamp, b[1].timestamp)),
        reverse=True
      )
      self.cache = dict(sortedcache[:self.opts.cachelower])

  def worker(self):
    while True:
      with self.cachelock:
        dirties = list(self.dirty)
        if self.workerstop and not dirties:
          return
      for bid in dirties:
        with self.cachelock:
          data = self.cache[bid].data
          self.dirty.remove(bid)
        self.s3.set("%s/block-%d" % (self.opts.prefix, bid),
                    zlib.compress(data))
      time.sleep(0.1)

  def flushall(self):
    pass

if __name__ == '__main__':
  s3bd = S3BD()
  s3bd.run()
