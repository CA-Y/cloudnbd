#!/usr/bin/env python
#
# blocktree.py - Interface between S3/Local cache and the high level
#                logic of S3BD
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
import s3nbd

class BlockTree(object):
  """Interface between S3/Local cache and the high level logic."""
  def __init__(self, pass_key = None, crypt_key = None, s3 = None):
    self.pass_key = pass_key
    self.crypt_key = crypt_key
    self.s3 = s3

  def set(self, path, data, start = None, end = None, direct = False):
    pass

  def get(self, path, start = None, end = None, direct = False):
    pass
