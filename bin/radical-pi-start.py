#!/usr/bin/env python3

__copyright__ = 'Copyright 2017-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.pi as rpi


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    server = None
    try:
        server = rpi.PIServer()
        server.start()

    finally:
        if server:
            server.terminate()

# ------------------------------------------------------------------------------

