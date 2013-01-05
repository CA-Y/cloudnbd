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
import cnbdcore
import os
from cnbdcore import nbd
from cnbdcore.cmd import fatal, warning, info, get_all_creds

class InfoCMD(object):
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

    # print the relevant info to standard output

    print('size:         %s' \
      % cnbdcore.size_to_hum(self.config['size']))
    print('block size:   %s' \
      % cnbdcore.size_to_hum(self.config['bs']))

def main(args):
  get_all_creds(args)
  infocmd = InfoCMD(args)
  infocmd.run()

