#!/usr/bin/env python
#
# listcmd.py - List the currently open volumes
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
import sys
import glob
from cloudnbd.cmd import fatal, warning, info, get_all_creds

def main(args):

  paths = filter(lambda a: a['ext'] == 'pid', cloudnbd.get_stat_paths())
  lst = []
  for path in paths:
    try:
      pid = int(open(path['path'], 'r').read())
      lst.append((path['backend'], path['bucket'], path['volume']))
    except:
      pass
  lst.sort(cmp=lambda a, b: cmp(''.join(a), ''.join(b)))
  lst = [('[backend]', '[bucket]', '[volume]')] + lst
  lst_len = map(lambda a: map(lambda b: len(b), a), lst)
  max_len = map(lambda a: max(*a), map(lambda *r: list(r), *lst_len))
  fmt = ' '.join(map(lambda a: '%%-%ds' % a, max_len))
  for i in lst:
    print(fmt % i)
