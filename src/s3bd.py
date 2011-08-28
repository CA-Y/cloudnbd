#!/usr/bin/env python
#
# s3bd.py - Main executable of s3bd
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
import sys
import argparse
import S3NBD

def main():
  commands = ['init', 'open', 'close', 'resize', 'closeall',
              'info', 'stat', 'list']
  if len(sys.argv) > 1 and sys.argv[1] in commands:
    cmd = sys.argv[1]
    del sys.argv[1]
    sys.argv[0] += ' ' + cmd
    exec('import s3bd_%s' % cmd)
    exec('s3bd_%s.main()' % cmd)
    exit(0)
  
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="NBD server with Amazon S3 as the backend",
    epilog="""Following is a short description of each command:
  close:    close an open volume
  closeall: close all open volumes
  info:     show volume info of an S3 volume
  init:     initialize a new S3 volume
  list:     list the currently open volumes
  open:     open an existing volume
  resize:   set the default size for an S3 volume
  stat:     show the statistics of an open S3 volume

For more info about each command, use '-h'.
"""
  )
  parser.add_argument(
    'command',
    type=str,
    choices=commands,
    help="command to run - for more info about each, add -h"
  )
  parser.add_argument('-v', '--version', action='store_true')
  args = parser.parse_args()

  if args.version:
    print(S3NBD.__version__)

if __name__ == '__main__':
  main()
