#!/usr/bin/env python3

import radical.pilot as rp
import radical.pi    as rpi


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    sid = 'foo.1'
    url = 'http://rct:lacidar@0.0.0.0:8090/'

    print('login')
    pi = rpi.PI(url=url)

    print('create session (sid: %s)' % sid)
    pi.sessions_create(sid)

    print('inspect sessions')
    print('sessions: ', pi.sessions_inspect())

    print('inspect pilots (no pilots)')
    info = pi.pilots_inspect(sid)
    for p in info:
        print('%s: %s' % (p['uid'], p['state']))
    print('ok')

    print('submit pilots (normal)')
    pi.pilots_submit(sid,
                     [{'resource': 'local.localhost',
                       'cores'   : 160,
                       'runtime' : 3}])

    print('inspect pilots (recently created)')
    info = pi.pilots_inspect(sid)
    for p in info:
        print('%s: %s' % (p['uid'], p['state']))

    print('wait pilots')
    pi.pilots_wait(sid, states=rp.PMGR_LAUNCHING)
    print('ok')


# ------------------------------------------------------------------------------

