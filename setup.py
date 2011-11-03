#!/usr/bin/env python
# setup.py - Installs the scripts to the system
#

from distutils.core import setup
import os
os.umask(022)

setup(
  name='cloudnbd',
  version='0.21',
  description='NBD server with cloud storage as backend',
  author='Mansour',
  author_email='mansour@oxplot.com',
  url='https://github.com/oxplot/cloudnbd',
  packages=['cnbdcore', 'cnbdcore.cloud', 'cnbdcore.cmd'],
  package_dir={
    'cnbdcore': 'src/cnbdcore',
    'cnbdcore.cmd': 'src/cnbdcore/cmd',
    'cnbdcore.cloud': 'src/cnbdcore/cloud'
  },
  scripts=['src/cloudnbd'],
  data_files=[
    ('/etc', ['cloudnbd.conf']),
    ('man/man1', ['docs/cloudnbd.1'])
  ],
  license='GPLv3+',
  requires=[
    'argparse',
    'boto',
    'errno',
    'fcntl',
    'getpass',
    'glob',
    'hashlib',
    'json',
    'os',
    're',
    'signal',
    'socket',
    'stat',
    'struct',
    'sys',
    'tempfile',
    'threading',
    'time',
    'urllib2',
    'zlib',
    'Crypto'
  ]
)
