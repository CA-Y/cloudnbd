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
    self.blocks = s3nbd.CacheDict()
    self.blocks.backercb = self.blocks_backercb
    self.blocks.max_entries = s3nbd._block_cache_size
    self.blocks.drop_ratio = s3nbd._block_cache_reduction_ratio
    self.refcnts = s3nbd.CacheDict()
    self.refcnts.backercb = self.refcnt_backercb
    self.refcnts.max_entries = s3nbd._refcnt_cache_size
    self.refcnts.drop_ratio = s3nbd._refcnt_cache_reduction_ratio
    self.bmp = s3nbd.CacheDict()
    self.bmp.backercb = self.bmp_backercb
    self.bmp.max_entries = s3nbd._bmp_cache_size
    self.bmp.drop_ratio = s3nbd._bmp_cache_reduction_ratio

  def blocks_backercb(self, key):
    pass

  def refcnt_backercb(self, key):
    pass

  def bmp_backercb(self, key):
    pass

  def nbd_readcb(self, off, length):
    pass

  def nbd_writecb(self, off, length):
    pass

  def nbd_closecb(self, off, length):
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
      s3=self.s3
    )

    # ensure there is a volume with the given name (config file exists)

    config = self.blocktree.get('config', cache='ignore')
    if not config:
      fatal("volume with name '%s' does not exist in bucket '%s'"
            % (args.volume, args.bucket))

    # load the config and get the encryption key

    self.config = s3nbd.deserialize_config(config)
    self.crypt_key = self.config['crypt_key'].decode('hex')
    self.blocktree.crypt_key = self.crypt_key

    # load the root config

    if not self.args.root:
      fatal("no root is specified")
    self.root_config = self.blocktree.get(
      'roots/%s/config' % self.args.root,
      cache='ignore'
    )

    self.root_config = s3nbd.deserialize_config(self.root_config)

    # set the reporting size for NBD

    if self.args.size is not None:
      self.nbd.size = self.args.size
    else:
      self.nbd.size = self.root_config['size']

    # start NBD server

    print('running server')
    self.nbd.run()

def main(args):
  get_all_creds(args)
  opencmd = OpenCMD(args)
  opencmd.run()

