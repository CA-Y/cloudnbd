#!/usr/bin/env python
#
# s3bd_init.py - Initialize a new S3 volume
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
#from S3NBD import auth
from S3NBD import cmdline

def main():
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Initialize a new S3 volume",
  )
  cmdline.add_common_args(parser)
  cmdline.add_name_args(parser)
  cmdline.add_size_arg(parser, as_arg=True)
  cmdline.add_auth_args(parser)

  args = parser.parse_args()

  if args.version:
    print(S3NBD.__version__)
    exit(0)

  print(repr(args))

if __name__ == '__main__':
  main()
