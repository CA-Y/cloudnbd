#!/usr/bin/env python
#
# nbd.py - Network Block Device server implementation
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
import struct
import socket
import threading

class NBDError(Exception):
  pass

def _default_cb(*args):
  pass

class NBD(object):

  READ = 0
  WRITE = 1
  CLOSE = 2

  def __init__(self,
               host = None,
               port = None,
               size = None,
               readcb = _default_cb,
               writecb = _default_cb,
               closecb = _default_cb):
    self.host = host
    self.port = port
    self.size = size
    self.readcb = readcb
    self.writecb = writecb
    self.closecb = closecb
    self._lock = threading.RLock()
    self._stats = {'reads': 0, 'writes': 0}
    self.interrupted = False

  def run(self):
    try:
      self._run()
    except socket.error as (errno, msg):
      if errno == 4:
        raise cloudnbd.Interrupted()
      else:
        raise

  def _run(self):
    self._lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self._lsock.bind((self.host, self.port))
    self._lsock.listen(1)
    sock, addr = self._lsock.accept()
    sock.send(b'NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53' +
      struct.pack(b'>Q', self.size) + b'\0' * 128)
    while not self.interrupted:
      header = self._receive(sock, struct.calcsize(b'>LL8sQL'))
      mag, request, han, off, dlen = struct.unpack(b'>LL8sQL', header)
      if mag != 0x25609513:
        raise NBDError("Invalid NBD magic sent by the client")
      if request == NBD.READ:
        with self._lock:
          self._stats['reads'] += 1
        sock.send(b'gDf\x98\0\0\0\0' + han)
        v = self.readcb(off, dlen)
        sock.send(self.readcb(off, dlen))
      elif request == NBD.WRITE:
        with self._lock:
          self._stats['writes'] += 1
        self.writecb(off, self._receive(sock, dlen))
        sock.send(b'gDf\x98\0\0\0\0' + han)
      elif request == NBD.CLOSE:
        sock.close()
        self.closecb()
        return
    if self.interrupted:
      raise cloudnbd.Interrupted()

  def _receive(self, sock, length):
    buf = []
    while length > 0:
      chunk = sock.recv(length)
      if not chunk:
        raise NBDError('Client unexpectedly closed the connection')
      buf.append(chunk)
      length -= len(chunk)
    return b''.join(buf)

  def get_stats(self):
    with self._lock:
      return dict(self._stats)
