#!/usr/bin/env python
#
# passwdcmd.py - Change the password on an existing volume
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

class PasswdCMD(object):
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

    # ensure there is a volume with the given name (config file exists)

    try:
      config = self.blocktree.get('config')
      if not config:
        fatal("volume with name '%s' does not exist"
              % (self.args.volume))
    except cnbdcore.blocktree.BTInvalidKey:
      fatal("decryption of config failed, most likely wrong"
            " passphrase supplied")

    # load the config and get the encryption key

    self.config = cnbdcore.deserialize(config)

    # ensure the volume is not being deleted

    if 'deleted' in self.config:
      fatal('volume set to be deleted')

    # write config back with the new password key

    new_pass_key = cnbdcore.auth.get_pass_key(self.args.new_passphrase)
    self.blocktree.pass_key = new_pass_key
    self.blocktree.set('config', config, direct=True)
    self.blocktree.close()

def main(args):
  get_all_creds(args)
  passwdcmd = PasswdCMD(args)
  passwdcmd.run()

