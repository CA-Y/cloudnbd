#!/usr/bin/env python
#
# statcmd.py - Print the statistics for a running server
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
import sys
from cnbdcore.cmd import fatal, warning, info, get_all_creds

def main(args):

  path = cnbdcore.get_stat_path(args.backend, args.volume)
  try:
    content = open(path, 'r').read()
  except:
    fatal('the requested volume does not seem to be open - use'
          ' \'%s list\' to get list of currently open volumes'
          % cnbdcore._prog_name)
  sys.stdout.write(content)
