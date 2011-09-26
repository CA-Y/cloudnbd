#!/usr/bin/env python
#
# infocmd.py - Show info about a volume as stored remotely
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
from cloudnbd import nbd
from cloudnbd.cmd import fatal, warning, info, get_all_creds

class InfoCMD(object):
  def __init__(self, args):
    self.args = args
    self.cloud = cloudnbd.cloud.backends[args.backend](
      access_key=args.access_key,
      bucket=args.bucket,
      volume=args.volume
    )

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
      threads=1
    )

    # ensure there is a volume with the given name (config file exists)

    try:
      config = self.blocktree.get('config')
      if not config:
        fatal("volume with name '%s' does not exist in bucket '%s'"
              % (self.args.volume, self.args.bucket))
    except cloudnbd.blocktree.BTInvalidKey:
      fatal("decryption of config failed, most likely wrong"
            " passphrase supplied")

    # load the config and get the encryption key

    self.config = cloudnbd.deserialize(config)

    # ensure the volume is not being deleted

    if 'deleted' in self.config:
      fatal('volume set to be deleted')

    # print the relevant info to standard output

    print('size:         %s' \
      % cloudnbd.size_to_hum(self.config['size']))
    print('block size:   %s' \
      % cloudnbd.size_to_hum(self.config['bs']))

def main(args):
  get_all_creds(args)
  infocmd = InfoCMD(args)
  infocmd.run()

