#!/usr/bin/env python
#
# __init__.py - Utility functions for use with command modules
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
import sys
import getpass

def fatal(msg):
  sys.stderr.write("%s:error: %s\n" % (cnbdcore._prog_name, msg))
  exit(1)

def warning(msg):
  sys.stderr.write("%s:warning: %s\n" % (cnbdcore._prog_name, msg))

def info(msg):
  sys.stderr.write("%s:info: %s\n" % (cnbdcore._prog_name, msg))

def get_all_creds(args):
  try:
    _get_all_creds(args)
  except (EOFError, KeyboardInterrupt):
    fatal('authentication cancelled')

def _get_all_creds(args):
  if args.access_key is None:
    args.access_key = raw_input('access key: ')
  if args.secret_key is None:
    args.secret_key = getpass.getpass('secret key: ')
  if args.passphrase is None:
    args.passphrase = getpass.getpass('passphrase: ')
  if hasattr(args, 'new_passphrase') and args.new_passphrase is None:
    same = False
    while not same:
      args.new_passphrase = getpass.getpass('new passphrase: ')
      confirm = getpass.getpass('confirm: ')
      same = args.new_passphrase == confirm
      if not same:
        print('PASSWORDS DO NOT MATCH - TRY AGAIN')

from cnbdcore.cmd import initcmd
from cnbdcore.cmd import closecmd
from cnbdcore.cmd import closeallcmd
from cnbdcore.cmd import deletecmd
from cnbdcore.cmd import opencmd
from cnbdcore.cmd import listcmd
from cnbdcore.cmd import statcmd
from cnbdcore.cmd import infocmd
from cnbdcore.cmd import passwdcmd
from cnbdcore.cmd import resizecmd
