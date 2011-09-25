#!/usr/bin/env python
#
# opencmd.py - Serve the cloud through an NBD server
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
import stat
import threading
import time
from cloudnbd import nbd
from cloudnbd.cmd import fatal, warning, info, get_all_creds

class OpenCMD(object):
  """Serve the cloud through an NBD server"""
  def __init__(self, args):
    self._serve_stat = False
    self.args = args
    self.cloud = cloudnbd.cloud.backends[args.backend](
      access_key=args.access_key,
      bucket=args.bucket,
      volume=args.volume
    )
    self.nbd = nbd.NBD(
      host=args.bind_address,
      port=args.port,
      readcb=self.nbd_readcb,
      writecb=self.nbd_writecb,
      closecb=self.nbd_closecb
    )

  def get_block(self, block):
    data = self.blocktree.get('blocks/%d' % block)
    return data if data else self.empty_block

  def set_block(self, block, data):
    self.blocktree.set('blocks/%d' % block, data)

  def nbd_readcb(self, off, length):
    bs = self.config['bs']
    block = off // bs
    start = off % bs
    end = (min(off + length, (block + 1) * bs) - 1) % bs + 1
    data = []
    while block * bs < off + length:
      data.append(self.get_block(block)[start:end])
      start = 0
      end = (min(off + length, (block + 2) * bs) - 1) % bs + 1
      block += 1
    return b''.join(data)

  def nbd_writecb(self, off, data):
    length = len(data)
    datap = 0
    bs = self.config['bs']
    block = off // bs
    start = off % bs
    end = (min(off + length, (block + 1) * bs) - 1) % bs + 1
    while block * bs < off + length:
      if end - start < bs:
        bd = self.get_block(block)
        bd = bd[:start] + data[datap:end - start + datap] \
          + bd[end:]
        self.set_block(block, bd)
      else:
        self.set_block(block, data[datap:end - start + datap])
      datap += end - start
      start = 0
      end = (min(off + length, (block + 2) * bs) - 1) % bs + 1
      block += 1

  def nbd_closecb(self):
    pass

  def _run_silent(self, t, *args, **kwargs):
    try:
      t(*args, **kwargs)
    except:
      pass

  def run(self):

    # check our access to Bridge

    try:
      self.cloud.check_access()
    except (cloudnbd.cloud.BridgeAccessDenied,
            cloudnbd.cloud.BridgeNoSuchBucket) as e:
      fatal(e.args[0])

    self.pass_key = cloudnbd.auth.get_pass_key(self.args.passphrase)
    self.blocktree = cloudnbd.blocktree.BlockTree(
      pass_key=self.pass_key,
      cloud=self.cloud,
      threads=self.args.threads
    )
    self.blocktree.read_ahead = self.args.read_ahead

    # ensure there is a volume with the given name (config file exists)

    config = self.blocktree.get('config')
    if not config:
      fatal("volume with name '%s' does not exist in bucket '%s'"
            % (self.args.volume, self.args.bucket))

    # load the config and get the encryption key

    self.config = cloudnbd.deserialize(config)
    self.crypt_key = self.config['crypt_key'].decode('hex')
    self.blocktree.crypt_key = self.crypt_key

    # set cache sizes

    total_cache = self.args.max_cache // self.config['bs']
    write_cache = (self.args.max_cache *
      cloudnbd._write_to_total_cache_ratio) // self.config['bs']
    flush_cache = (self.args.max_cache *
      cloudnbd._write_to_total_cache_ratio *
      cloudnbd._write_queue_to_flush_ratio) // self.config['bs']
    if total_cache < 1: total_cache = 1
    if write_cache < 1: write_cache = 1
    if flush_cache < 1: flush_cache = 1
    self.blocktree.set_cache_limits(
      total=total_cache,
      write=write_cache,
      flush=flush_cache
    )

    # set the reporting size for NBD

    if self.args.size is not None:
      self.nbd.size = self.args.size
    else:
      self.nbd.size = self.config['size']

    # empty block

    self.empty_block = b'\x00' * self.config['bs']

    # TODO a fork goes here

    # setup a unix socket for stat communication and pid file

    self._stat_path = cloudnbd.get_stat_path(
      self.args.backend, self.args.bucket, self.args.volume)
    self._pid_path = cloudnbd.get_pid_path(
      self.args.backend, self.args.bucket, self.args.volume)

    def create_stat():
      if os.path.exists(self._stat_path):
        os.unlink(self._stat_path)
      os.mknod(self._stat_path, 0644 | stat.S_IFIFO)
      self._serve_stat = True
    def create_pid():
      open(self._pid_path, 'w').write(str(os.getpid()))
    def delete_file(path):
      os.unlink(path)
    create_stat()
    self._run_silent(create_stat)
    self._run_silent(create_pid)

    try:
      self._run()
    finally:
      self._run_silent(delete_file, self._stat_path)
      self._run_silent(delete_file, self._pid_path)

  def _stat_server_worker(self):
    while True:
      try:
        f = open(self._stat_path, 'w')
        rstats = self.blocktree.get_stats()
        stats = {}
        stats['cache-used'] = self._size_to_human(
          rstats['cache_size'] * self.config['bs']
        )
        stats['cache-dirty'] = self._size_to_human(
          rstats['queue_size'] * self.config['bs']
        )
        stats['cache-limit'] = self._size_to_human(self.args.max_cache)
        stats['sent-reqs'] = str(rstats['sent_count'])
        stats['recv-reqs'] = str(rstats['recv_count'])
        stats['sent-data'] = self._size_to_human(rstats['data_sent'])
        stats['recv-data'] = self._size_to_human(rstats['data_recv'])
        stats['sent-actual'] = self._size_to_human(rstats['wire_sent'])
        stats['recv-actual'] = self._size_to_human(rstats['wire_recv'])
        stats = stats.items()
        stats.sort(cmp=lambda a, b: cmp(a[0], b[0]))
        f.write(''.join(map(lambda a: '%s: %s\n' % a, stats)))
        f.flush()
        f.close()
      except:
        pass
      time.sleep(0.5)

  def _size_to_human(self, size):
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

  def _run(self):

      # start a thread for stat

      if self._serve_stat:
        self._stat_thread = \
          threading.Thread(target=self._stat_server_worker)
        self._stat_thread.daemon = True
        self._stat_thread.start()

      # start NBD server

      print('running server')
      self.nbd.run()

      # wrap up

      print('committing cache before closing')
      self.blocktree.close()

def main(args):
  get_all_creds(args)
  opencmd = OpenCMD(args)
  opencmd.run()
