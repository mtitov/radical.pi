#!/usr/bin/env python3

import radical.pilot as rp
import radical.pi    as rpi


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    unum  = 5    # num tasks
    ugen  = 2    # generations
    usize = 4    # threads
    utime = 1    # sec

    sid = 'foo.1'
    url = 'http://rct:lacidar@0.0.0.0:8090/'

    print('login')
    pi = rpi.PI(url=url)

    print('inspect sessions')
    info = pi.pilots_inspect(sid)
    for p in info:
        print('%s: %s' % (p['uid'], p['state']))

    print('submit tasks')
    tasks = []
    for _ in range(unum):
        tasks.append({'executable'       : '/bin/date',
                      'cpu_processes'    : 1,
                      'cpu_threads'      : usize})
    tids = pi.tasks_submit(sid, tasks)
    print('tasks: ', tids)

    print('inspect tasks')
    info = pi.tasks_inspect(sid)
    for t in info:
        print('%s: %s [%s]' % (t['uid'], t['state'], t['stdout']))
    print('ok')

    print('wait for task completion')
    pi.tasks_wait(sid, states=rp.FINAL)
    print('ok')

    print('inspect tasks')
    info = pi.tasks_inspect(sid)
    for t in info:
        print('%s: %s: %s' % (t['uid'], t['state'], t['stdout']))
    print('ok')

    print('stdout for %s' % tids[0])
    print(pi.tasks_stdout(sid, tids[0]))

    try:
        print('stderr for %s' % tids[0])
        print(pi.tasks_stderr(sid, tids[0]))
    except Exception as e:
        print(e)


# ------------------------------------------------------------------------------

