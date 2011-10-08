#!/usr/bin/env python
#
# compress.py - Compression libraries
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
#import lzma
import zlib

class Compressor(object):
  def compress(self, data):
    raise NotImplemented('abstract class')
  def decompress(self, data):
    raise NotImplemented('abstract class')

class LZMA(Compressor):
  def compress(self, data):
    return lzma.compress(data, options={'level': 9})
  def decompress(self, data):
    return lzma.decompress(data)

class Deflate(Compressor):
  def compress(self, data):
    return zlib.compress(data, 9)
  def decompress(self, data):
    return zlib.decompress(data)

class Plain(Compressor):
  def compress(self, data):
    return data
  def decompress(self, data):
    return data

compressors = {
  'deflate': Deflate,
  'plain': Plain
}
