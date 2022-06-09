
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

# ------------------------------------------------------------------------------
#
from .client import PI
from .server import PIServer

# ------------------------------------------------------------------------------
#
import radical.utils as _ru
import os            as _os

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version(_os.path.dirname(__file__))

version = version_short

# ------------------------------------------------------------------------------

