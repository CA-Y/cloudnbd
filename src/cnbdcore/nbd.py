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
import cnbdcore
import struct
import socket
import threading

class NBDError(Exception):
  pass

def _default_cb(*args):
  return (0, '')

class NBD(object):

  CMD_READ = 0
  CMD_WRITE = 1
  CMD_DISC = 2
  CMD_FLUSH = 3
  CMD_TRIM = 4
  FLAG_HAS_FLAGS  = 0b000001
  FLAG_READ_ONLY  = 0b000010
  FLAG_SEND_FLUSH = 0b000100
  FLAG_SEND_FUA   = 0b001000
  FLAG_ROTATIONAL = 0b010000
  FLAG_SEND_TRIM  = 0b100000

  def __init__(self,
               host = None,
               port = None,
               size = None,
               readcb = _default_cb,
               writecb = _default_cb,
               closecb = _default_cb,
               flushcb = _default_cb,
               trimcb = _default_cb):
    self.host = host
    self.port = port
    self.size = size
    self.readcb = readcb
    self.writecb = writecb
    self.closecb = closecb
    self.flushcb = flushcb
    self.trimcb = trimcb
    self._lock = threading.RLock()
    self._stats = {
      NBD.CMD_READ: 0,
      NBD.CMD_WRITE: 0,
      NBD.CMD_DISC: 0,
      NBD.CMD_FLUSH: 0,
      NBD.CMD_TRIM: 0
    }
    self.interrupted = False

  def run(self):
    try:
      self._run()
    except socket.error as (errno, msg):
      if errno == 4:
        raise cnbdcore.Interrupted()
      else:
        raise

  def _run(self):
    self._lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self._lsock.bind((self.host, self.port))
    self._lsock.listen(1)
    sock, addr = self._lsock.accept()
    flags = NBD.FLAG_HAS_FLAGS | NBD.FLAG_SEND_FLUSH \
          | NBD.FLAG_SEND_TRIM
    sock.send(b'NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53' +
      struct.pack(b'>QL', self.size, flags) + b'\0' * 124)

    while not self.interrupted:
      header = self._receive(sock, struct.calcsize(b'>LL8sQL'))
      mag, request, han, off, dlen = struct.unpack(b'>LL8sQL', header)
      if mag != 0x25609513:
        raise NBDError("Invalid NBD magic sent by the client")

      # update the stats

      with self._lock:
        self._stats[request] += 1

      if request == NBD.CMD_READ:
        err, data = self.readcb(off, dlen)
        sock.send(struct.pack(b'>4sL8s', b'gDf\x98', err, han))
        sock.send(data)
      elif request == NBD.CMD_WRITE:
        data = self._receive(sock, dlen)
        err, data = self.writecb(off, data)
        sock.send(struct.pack(b'>4sL8s', b'gDf\x98', err, han))
      elif request == NBD.CMD_DISC:
        sock.close()
        self.closecb()
        return
      elif request == NBD.CMD_FLUSH:
        err, data = self.flushcb(off, dlen)
        sock.send(struct.pack(b'>4sL8s', b'gDf\x98', err, han))
      elif request == NBD.CMD_TRIM:
        err, data = self.trimcb(off, dlen)
        sock.send(struct.pack(b'>4sL8s', b'gDf\x98', err, han))

    if self.interrupted:
      raise cnbdcore.Interrupted()

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
