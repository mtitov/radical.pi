#!/usr/bin/env python3

import radical.pi as rpi


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    sid   = 'foo.0'
    url   = 'http://rct:lacidar@0.0.0.0:8090/'

    print('login')
    pi = rpi.PI(url=url)

    print('inspect sessions')
    print(pi.sessions_inspect())

    print('create session')
    pi.sessions_create(sid)
    print('sid: %s' % sid)

    print('inspect sessions')
    print(pi.sessions_inspect())

    print('close sessions')
    print(pi.sessions_close(sid))

# ------------------------------------------------------------------------------

