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
from __future__ import division
import sys
import argparse
import s3nbd

def main():

  if any(map(lambda a: unicode(a) in ['-v', '--version'],
         sys.argv[1:])):
    print(s3nbd._print_ver)
    exit(0)

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="NBD server with Amazon S3 as the backend"
  )
  _add_common_args(parser)
  subparsers = parser.add_subparsers(
    help="s3bd commands",
    dest="command"
  )

  # close arguments
  parser_a = subparsers.add_parser(
    'close',
    help='close an open volume'
  )
  _add_name_args(parser_a)
  _add_close_cow_arg(parser_a)

  # closeall arguments
  parser_a = subparsers.add_parser(
    'closeall',
    help='close all open volumes'
  )
  _add_close_cow_arg(parser_a)

  # info arguments
  parser_a = subparsers.add_parser(
    'info',
    help='show volume details'
  )
  _add_name_args(parser_a)
  _add_auth_args(parser_a)

  # init arguments
  parser_a = subparsers.add_parser(
    'init',
    help='initialize a new volume'
  )
  _add_name_args(parser_a)
  _add_size_arg(parser_a, as_arg=True)
  _add_blocksize_arg(parser_a)
  _add_auth_args(parser_a)
  
  # list arguments
  parser_a = subparsers.add_parser(
    'list',
    help='list the currently open volumes'
  )
  
  # open arguments
  parser_a = subparsers.add_parser(
    'open',
    help='open an existing volume'
  )
  _add_name_args(parser_a)
  _add_size_arg(parser_a)
  _add_server_args(parser_a)
  _add_auth_args(parser_a)
  parser_a.add_argument(
    '-c', '--cow',
    action='store_true',
    help="COW all the changes to the volume"
  )
  parser_a.add_argument(
    '-t', '--threads',
    type=int,
    metavar='<count>',
    default=s3nbd._default_write_thread_count,
    help="number of write threads (default: %d)" \
          % s3nbd._default_write_thread_count
  )
  parser_a.add_argument(
    '-r', '--read-ahead',
    type=int,
    metavar='<count>',
    default=s3nbd._default_read_ahead_count,
    help="number of blocks to read ahead (default: %d)" \
          % s3nbd._default_read_ahead_count
  )
  parser_a.add_argument(
    '-e', '--max-cache',
    type=_storage_size,
    default=s3nbd._default_total_cache_size,
    metavar="<size>",
    help="maximum amount of in-memory cache to use -"
         " e.g. 100M which is 100 megabytes (default: %d)" \
          % s3nbd._default_total_cache_size
  )

  # resize arguments
  parser_a = subparsers.add_parser(
    'resize',
    help='set the default size for a volume'
  )
  _add_name_args(parser_a)
  _add_size_arg(parser_a, as_arg=True)
  _add_auth_args(parser_a)

  # stat arguments
  parser_a = subparsers.add_parser(
    'stat',
    help='show statistics about a currently open volume'
  )
  _add_name_args(parser_a)

  args = parser.parse_args()

  exec ('import s3nbd.cmd.%scmd' % args.command) in locals(), globals()
  exec ('s3nbd.cmd.%scmd.main(args)' % args.command) \
    in locals(),globals()

def _storage_size(value):
  """Parse a size with possible letter multipliers and return the actual
  value as integer.
  """
  import re
  m = re.match(ur'(\d+)(.?)', value.strip().lower())
  if m and m.group(2) in list('kmgtp') + ['']:
    im = {'': 1e0, 'k': 1e3, 'm': 1e6,
          'g': 1e9, 't': 1e12, 'p': 1e15}
    return int(int(m.group(1)) * im[m.group(2)])
  else:
    msg = "%s must be in the form: <int><K|M|G|T|P>"
    raise argparse.ArgumentTypeError(msg)

def _add_name_args(parser):
  """Add volume bucket and name arguments to the parser."""
  parser.add_argument(
    'bucket',
    metavar='<bucket>',
    type=unicode,
    help="S3 bucket name"
  )
  parser.add_argument(
    'volume',
    metavar='<volume>',
    type=unicode,
    help="name of the volume"
  )

def _add_close_cow_arg(parser):
  """Add close COW arguments to the parser."""
  parser.add_argument(
    '--cow', '-c',
    metavar='<action>',
    choices=['discard', 'apply'],
    help="action to take for the outstanding COW data"
         " - use 'apply' to merge the COW data to the volume"
         ", or use 'discard' to drop all the changes"
  )

def _add_size_arg(parser, as_arg = False):
  """Add size argument to the parser.

  Parameters:
    as_arg - if True, makes size a required positional argument instead
             of an optional one
  """
  parser.add_argument(
    *(['size'] if as_arg else ['-s', '--size']),
    type=_storage_size,
    metavar="<size>",
    help="size of volume as reported to NBD client -"
         " e.g. 100T which is 100 terabytes"
  )

def _add_blocksize_arg(parser):
  """Add block size argument to the parser."""
  parser.add_argument(
    '-b', '--block-size',
    metavar="<size>",
    type=_storage_size,
    help="block size of blocks as stored on S3 - e.g. 100T which is"
         " 100 terabytes (default: %d)" % s3nbd._default_bs
  )

def _add_server_args(parser):
  """Add NBD server related arguments to parser."""
  parser.add_argument(
    '-i', '--bind-address',
    metavar="<ip>",
    default=s3nbd._default_bind,
    type=unicode,
    help="the IP address the NBD server will be bound to"
         " (default: all interfaces)"
  )
  parser.add_argument(
    '-p', '--port',
    metavar="<port>",
    default=s3nbd._default_port,
    type=int,
    help="the port the NBD server will listen on"
         " (default: %d)" % s3nbd._default_port
  )

def _add_auth_args(parser):
  """Add authentication related arguments to the parser."""
  parser.add_argument(
    '-a', '--access-key',
    metavar="<access-key>",
    type=unicode,
    help="S3 access key"
  )
  parser.add_argument(
    '-k', '--secret-key',
    metavar="<secret-key>",
    type=unicode,
    help="S3 secret key"
  )
  parser.add_argument(
    '-y', '--passphrase',
    metavar="<passphrase>",
    type=unicode,
    help="passphrase used to encrypt data on S3"
  )

def _add_common_args(parser):
  """Add common arguments to the parser."""
  parser.add_argument(
    '-v', '--version',
    action='store_true',
    help="show the program version and exit"
  )

if __name__ == '__main__':
  main()
