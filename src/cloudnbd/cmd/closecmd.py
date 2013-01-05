#!/usr/bin/env python
#
# closecmd.py - Close a currently open volume
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
import signal
from cloudnbd.cmd import fatal, warning, info, get_all_creds

def main(args):

  pid_path = cloudnbd.get_pid_path(
    args.backend, args.bucket, args.volume
  )
  try:
    pid = int(open(pid_path, 'r').read())
    os.kill(pid, signal.SIGINT)
  except:
    fatal('the given volume is not open - use \'%s list\' to see'
          ' list of currently open volumes' % cloudnbd._prog_name)

