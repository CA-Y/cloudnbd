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
import cnbdcore
import os
import sys
import glob
from cnbdcore.cmd import fatal, warning, info, get_all_creds

def main(args):

  vol_ids = cnbdcore.get_open_volumes_list()
  lst = []
  for vid in vol_ids:
    if cnbdcore.acquire_pid_lock(*vid):
      cnbdcore.release_pid_lock(*vid)
    else:
      lst.append(vid)
  if lst:
    lst.sort(cmp=lambda a, b: cmp(''.join(a), ''.join(b)))
    lst = [('[backend]', '[volume]')] + lst
    lst_len = map(lambda a: map(lambda b: len(b), a), lst)
    max_len = map(lambda a: max(*a), map(lambda *r: list(r), *lst_len))
    fmt = ' '.join(map(lambda a: '%%-%ds' % a, max_len))
    for i in lst:
      print(fmt % i)
