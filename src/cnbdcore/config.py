#!/usr/bin/env python
#
# config.py - Config file related utilities
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

_allowed_config_keys = ['access_key', 'secret_key', 'passphrase']

class ConfigParseError(Exception):
  pass

def parse(path = None):
  entries = {}
  if path is None:
    entries.update(_load_file(cnbdcore._global_config_path))
    entries.update(_load_file(cnbdcore._user_config_path))
  elif not os.path.exists(path):
    raise ConfigParseError("'%s' config not found" % path)
  else:
    entries.update(_load_file(path))
  return entries

def _load_file(path):
  if os.path.exists(path):
    return _load_file_helper(path)
  return {}

def _load_file_helper(path):
  line = 0
  entries = {}
  k, v = None, None
  for l in open(path, 'r'):
    line += 1
    l = l.strip()
    if not l or l.startswith('#'):
      continue
    if l.startswith('[') and l.endswith(']'):
      if ':' not in l:
        raise ConfigParseError(
          "expected [backend:volume] on line %d in '%s'" % (line, path)
        )
      if k: entries[k] = v
      k, v = tuple(map(unicode.strip, l[1:-1].split(':', 1))), {}
      continue
    elif '=' not in l:
      raise ConfigParseError(
        "expected key=value pair on line %d in '%s'" % (line, path)
      )
    pl, pr = map(unicode.strip, l.split('=', 1))
    v[pl] = pr
  if k: entries[k] = v
  return entries

def underlay(args, cfg):
  if not hasattr(args, 'backend') or not hasattr(args, 'volume'):
    return
  volid = (args.backend, args.volume)
  if volid not in cfg:
    return
  for k in filter(lambda a: a in _allowed_config_keys
                            and hasattr(args, a), cfg[volid]):
    setattr(args, k, cfg[volid][k])
