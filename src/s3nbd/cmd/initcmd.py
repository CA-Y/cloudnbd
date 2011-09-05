#!/usr/bin/env python
#
# initcmd.py - Initialize a new S3 volume
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
from s3nbd.cmd import fatal, warning, info, get_all_creds

def main(args):

  get_all_creds(args)

  s3 = s3nbd.s3.S3(
    access_key=args.access_key,
    secret_key=args.secret_key,
    bucket=args.bucket,
    volume=args.volume
  )

  # check our access to S3

  try:
    s3.check_access()
  except (s3nbd.s3.S3AccessDenied, s3nbd.s3.S3NoSuchBucket) as e:
    fatal(e.args[0])

  pass_key = s3nbd.auth.get_pass_key(args.passphrase)
  crypt_key = s3nbd.auth.gen_crypt_key()
  blocktree = s3nbd.blocktree.BlockTree(
    pass_key=pass_key,
    crypt_key=crypt_key,
    s3=s3
  )

  # ensure no volume with the same name exists
  
  tmpconfig = blocktree.get('config', cache='ignore')
  if tmpconfig:
    fatal("volume '%s' in bucket '%s' already exists"
          % (args.volume, args.bucket))

  # set up the config

  volume_config = {
    'bs': s3nbd._default_bs,
    'bmp_bs': s3nbd._default_bmp_bs,
    'refcnt_bs': s3nbd._default_refcnt_bs,
    'crypt_key': crypt_key.encode('hex'),
    'next_block': 0
  }

  root_config = {
    'size': args.size
  }

  volume_config = s3nbd.serialize_config(volume_config)
  root_config = s3nbd.serialize_config(root_config)

  # to avoid using transactions here, we write the config for the root
  # first before the volume - that way only when the volume is written
  # we know that root config is also written
  
  blocktree.set(
    'roots/%s/config' % args.root,
    root_config,
    direct=True,
    dont_cache=True
  )
  blocktree.set('config', volume_config, direct=True, dont_cache=True)
