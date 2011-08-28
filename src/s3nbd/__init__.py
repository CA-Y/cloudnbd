"""
TODO
"""

__prog_name__ = 's3bd'
__ver_major__ = 0
__ver_minor__ = 1
__ver_patch__ = 0
__ver_sub__ = ''
__ver_tuple__ = (__ver_major__, __ver_minor__,
                 __ver_patch__, __ver_sub__)

__version__ = '%d.%d.%d%s' % __ver_tuple__

__print_ver__ = '%s %s' % (__prog_name__, __version__)

from s3nbd import cmd
from s3nbd import auth
