#!/usr/bin/env python
#
# s3bd.py - NBD server with Amazon S3 cloud storage system as the
#           backend
# Copyright (C) Mansour <mansour@oxplot.com>
#

import boto

class Options:
  def __init__(self):
    self.debug = False
    self.blocksize = 1024 * 4
    self.totalsize = None
    self.bucket = None
    self.prefix = None
    self.accesskey = None
    self.secretkey = None

class S3BD:

  def __inti__(self):
    self.opts = {
      'debug': False,
      'blocksize': 1024 * 4,
      'totalsize': None,
      'bucket': None,
      'prefix': None,
      'accesskey': None,
      'secretkey': None
    }

  def parseopts(self):
    args = sys.argv[1:]
    if '-D' in args:
      self.opts['debug'] =
    k

  def run(self):
    self.parseopts()
    # TODO

if __name__ == '__main__':
  s3bd = S3BD()
  s3bd.run()
