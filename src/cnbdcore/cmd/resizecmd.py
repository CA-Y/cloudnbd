#!/usr/bin/env python
#
# resizecmd.py - Resize the volume deleting all the unused blocks
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
import tempfile
import sys
import threading
from cnbdcore import nbd
from cnbdcore.cmd import fatal, warning, info, get_all_creds

class ResizeCMD(object):
  def __init__(self, args):
    self.args = args
    self.cloud = cnbdcore.cloud.backends[args.backend](
      access_key=args.access_key,
      secret_key=args.secret_key,
      volume=args.volume
    )

  def run(self):

    # check our access to Bridge

    try:
      self.cloud.check_access()
    except (cnbdcore.cloud.BridgeAccessDenied,
            cnbdcore.cloud.BridgeNoSuchBucket) as e:
      fatal(e.args[0])

    self.pass_key = cnbdcore.auth.get_pass_key(self.args.passphrase)
    self.blocktree = cnbdcore.blocktree.BlockTree(
      pass_key=self.pass_key,
      cloud=self.cloud,
      threads=1
    )

    # load the config

    self.config = cnbdcore.cmd.load_cloud_config(self.blocktree)

    # ensure the volume is not being deleted

    if 'deleted' in self.config:
      fatal('volume set to be deleted')

    if self.args.cleanup:
      print('\nNOTE resizing with cleanup option on will result in'
            ' all unused blocks to be deleted. Past this point,'
            ' any data residing beyond the size you specified will'
            ' become corrupted and eventually completely deleted.')
      if raw_input('To continute, type yes in uppercase: ') != 'YES':
        fatal('resize with cleanup aborted')

    # save the new size if given

    if self.args.size:
      old_size = cnbdcore.size_to_hum(self.config['size'])
      print('resizing from %s to %s'
            % (old_size, cnbdcore.size_to_hum(self.args.size)))
      self.config['size'] = self.args.size
      self.blocktree.set('config', cnbdcore.serialize(self.config),
                         direct=True)
      print('metadata updated')
    else:
      print('keeping size %s' % cnbdcore.size_to_hum(self.args.size))

    if not self.args.cleanup:
      print('resize completed with no cleanup')
      return

    # store the block number of all blocks to a file to avoid concurrent
    # access issues

    self._cachefile = tempfile.TemporaryFile()
    self._item_count = 0
    last_block = (self.config['size'] // self.config['bs']) + 1

    _print_caching_progress(self._item_count)
    for k in self.cloud.list(prefix='blocks/'):
      block_num = int(k.name.split('/')[-1])
      if block_num > last_block:
        self._cachefile.write('%s\n' % k.name.split('/')[-1])
        if self._item_count % 10 == 0:
          _print_caching_progress(self._item_count)
        self._item_count += 1
    _print_caching_progress(self._item_count)
    print()

    # start deleting the objects

    self._cachefile.seek(0)
    self._delete_count = 0
    self._delete_lock = threading.RLock()
    self._blocks_to_delete = self._get_blocks_to_delete()

    threads = []
    for i in xrange(self.args.threads):
      t = threading.Thread(target=self._delete_worker_factory())
      t.daemon = True
      threads.append(t)
      t.start()

    for t in threads:
      t.join()
    print()
    print('object cleanup completed')
    print('resize completed with object cleanup')

  def _get_blocks_to_delete(self):
    for l in self._cachefile:
      yield l.strip()

  def _delete_worker_factory(self):
    cloud = self.cloud.clone()
    def delete_worker():
      while True:
        try:
          with self._delete_lock:
            k = self._blocks_to_delete.next()
          cloud.delete('blocks/%s' % k)
          with self._delete_lock:
            self._delete_count += 1
            if self._delete_count % (self._item_count // 110) == 0:
              _print_deleting_progress(
                self._item_count, self._delete_count)
        except StopIteration:
          return
    return delete_worker

def _print_caching_progress(item_count):
  sys.stdout.write(
    '\x1b[2K\x1b[1Gcaching the list of obj to cleanup ... %d'
    % item_count
  )
  sys.stdout.flush()

def _print_deleting_progress(total, current):
  sys.stdout.write(
    '\x1b[2K\x1b[1Gcleaning up objects ... %d%%'
    % int(current / total * 100)
  )
  sys.stdout.flush()

def main(args):
  get_all_creds(args)
  resizecmd = ResizeCMD(args)
  resizecmd.run()

