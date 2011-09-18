#!/usr/bin/env python
#
# opencmd.py - Serve the S3 through an NBD server
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
from s3nbd import nbd
from s3nbd.cmd import fatal, warning, info, get_all_creds

class OpenCMD(object):
  """Serve the S3 through an NBD server"""
  def __init__(self, args):
    self.args = args
    self.s3 = s3nbd.s3.S3(
      access_key=args.access_key,
      secret_key=args.secret_key,
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

  def run(self):

    # check our access to S3

    try:
      self.s3.check_access()
    except (s3nbd.s3.S3AccessDenied, s3nbd.s3.S3NoSuchBucket) as e:
      fatal(e.args[0])

    self.pass_key = s3nbd.auth.get_pass_key(self.args.passphrase)
    self.blocktree = s3nbd.blocktree.BlockTree(
      pass_key=self.pass_key,
      s3=self.s3,
      threads=self.args.threads
    )

    # ensure there is a volume with the given name (config file exists)

    config = self.blocktree.get('config')
    if not config:
      fatal("volume with name '%s' does not exist in bucket '%s'"
            % (self.args.volume, self.args.bucket))

    # load the config and get the encryption key

    self.config = s3nbd.deserialize(config)
    self.crypt_key = self.config['crypt_key'].decode('hex')
    self.blocktree.crypt_key = self.crypt_key

    # set cache sizes

    total_cache = self.args.max_cache // self.cache['bs']
    write_cache = (self.args.max_cache *
      s3nbd._write_to_total_cache_ratio) // self.cache['bs']
    if total_cache < 1: total_cache = 1
    if write_cache < 1: write_cache = 1
    self.blocktree.set_cache_limits(total_cache, write_cache)

    # set the reporting size for NBD

    if self.args.size is not None:
      self.nbd.size = self.args.size
    else:
      self.nbd.size = self.config['size']

    # empty block

    self.empty_block = b'\x00' * self.config['bs']

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

