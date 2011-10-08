#!/usr/bin/env python
#
# initcmd.py - Initialize a new cloud volume
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
from cnbdcore.cmd import fatal, warning, info, get_all_creds

def main(args):

  get_all_creds(args)

  cloud = cnbdcore.cloud.backends[args.backend](
    access_key=args.access_key,
    secret_key=args.secret_key,
    volume=args.volume
  )

  # check our access to bridge

  try:
    cloud.check_access()
  except (cnbdcore.cloud.BridgeAccessDenied,
          cnbdcore.cloud.BridgeNoSuchBucket) as e:
    fatal(e.args[0])

  pass_key = cnbdcore.auth.get_pass_key(args.passphrase)
  crypt_key = cnbdcore.auth.gen_crypt_key()
  blocktree = cnbdcore.blocktree.BlockTree(
    pass_key=pass_key,
    crypt_key=crypt_key,
    cloud=cloud,
    threads=0
  )

  # ensure no volume with the same name exists
  
  try:
    config = blocktree.get('config')
    if config:
      fatal("volume '%s' already exists" % (args.volume))
  except cnbdcore.blocktree.BTDecryptError:
    fatal("volume '%s' already exists" % (args.volume))

  # set up the config

  config = cnbdcore.serialize({
    'bs': cnbdcore._default_bs,
    'crypt_key': crypt_key.encode('hex'),
    'size': args.size,
    'requires': ['compress-' + cnbdcore._default_compressor]
  })
  blocktree.set('config', config, direct=True)
  blocktree.close()
