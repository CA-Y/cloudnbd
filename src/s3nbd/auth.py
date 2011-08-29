#!/usr/bin/env python
#
# auth.py - Crypto/Authentication related facilities
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

def get_pass_key(passphrase):
  """Create a one to one cryptographic key for the given plain text
  password.
  """
  from hashlib import sha256
  return sha256(s3nbd._salt + passphrase).digest()

def gen_crypt_key():
  """Generate a new cryptographic key from random source.
  """
  import Crypto.Random
  gen = Crypto.Random.new()
  key = gen.read(32) # FIXME magic number
  gen.close()
  return key
