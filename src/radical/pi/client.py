
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import json
import requests

import radical.utils as ru

from .constants import PACKAGE_NS


# ------------------------------------------------------------------------------
#
class PI:
    """PI client class - RESTful API for `PilotClient`.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, url, log=None, prof=None, rep=None):

        if log : self._log  = log
        else   : self._log  = ru.Logger(PACKAGE_NS)

        if prof: self._prof = prof
        else   : self._prof = ru.Profiler(PACKAGE_NS)

        if rep : self._rep  = log
        else   : self._rep  = ru.Reporter(PACKAGE_NS)

        self._cookies       = []
        self._url           = ru.Url(url)
        self._qbase         = ru.Url(url)
        self._qbase         = str(self._qbase).rstrip('/')

        if self._url.username and self._url.password:
            self.login(self._url.username, self._url.password)

    # --------------------------------------------------------------------------
    #
    def _query(self, mode, route, data=None):

        url = self._qbase + route

        ldata = -1
        if data is not None:
            ldata = len(str(data))

        print('---> %-5s  %-60s [data:%d]' % (mode.upper(), url, ldata))

        self._log.debug('request %5s: %s [%s]', mode, route, data)
        self._log.debug('request %5s: %s', mode, url)

        if mode == 'get':
            r = requests.get(url, cookies=self._cookies) #, json=data)

        elif mode == 'put':
            r = requests.put(url, cookies=self._cookies, json=data)

        elif mode == 'post':
            r = requests.post(url, cookies=self._cookies, json=data)

        elif mode == 'delete':
            r = requests.delete(url, cookies=self._cookies, json=data)

        else:
            raise ValueError('invalid query mode %s' % mode)

        self._log.debug('reply   %3s: %s [%s]', r.status_code,
                                                 len(r.content), r.content[:64])

        if r.status_code != 200:
            raise RuntimeError('query failed:\n %s' % r.content)

        if r.cookies:
            assert(not self._cookies), 'we allow auth only once'
            self._cookies = r.cookies

        try:
            result = json.loads(r.content)

        except ValueError as e:
            raise RuntimeError('query failed: %s' % repr(e))

        print('     %-6s [%s]' % (result['success'], result.get('error', '')))
        if not result['success']:
            raise RuntimeError('query failed: %s' % result['error'])

        return result['result']

    # --------------------------------------------------------------------------
    #
    def login(self, username=None, password=None):
        """
        login to the service with given username and password.  This method will
        stor a cookie with a session secret so that future calls on this NGE
        object instance use the same credentials.  Another call to `login` will
        overwrite that cookie and use the new credentials.
        """
        username = username or self._url.username
        password = password or self._url.password
        return self._query('put', '/login/', {'username': username,
                                              'password': password})

    # --------------------------------------------------------------------------
    #
    def logout(self):
        """
        delete all sessions, terminate all pilots, invalidate the cookie.
        """
        return self._query('put', '/logout/')

    # --------------------------------------------------------------------------
    #
    def sessions_create(self, sid):
        """
        create named session.
        This will raise an error if the session already exists.
        """
        return self._query('put', '/sessions/%s/' % sid)

    # --------------------------------------------------------------------------
    #
    def sessions_inspect(self):
        """
        return all session IDs known for this user (irrespective of session
        state)
        """
        return self._query('get', '/sessions/')

    # --------------------------------------------------------------------------
    #
    def sessions_close(self, sid):
        """
        close the given session,  terminate all pilots and tasks.
        """
        return self._query('delete', '/sessions/%s/' % sid)

    # --------------------------------------------------------------------------
    #
    def pilots_submit(self, sid, descriptions):
        """
        request pilots, either as backfill or batch queue pilots.  This call
        will return a list of pilot IDs.
        """
        if not descriptions:
            return []

        args = ['put', '/sessions/%s/pilots/' % sid, ru.as_list(descriptions)]
        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def pilots_inspect(self, sid, pids=None):
        """
        return information about all pilots
        """
        pids = ru.as_list(pids)

        args = ['get', '/sessions/%s/pilots/' % sid]
        if pids and len(pids) == 1 and pids[0]:
            args[1] += '%s/' % pids[0]
        elif pids:
            args.append({'pids': pids})

        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def pilots_wait(self, sid, pids=None, states=None, timeout=None):
        """
        wait for a specific (set of) states for all pilots with the given UIDs
        (or for all known resources if no UID is specified).  This call will
        return after a given timeout, or after any of the given states have been
        reached, whichever occurs first.  A negative timeout value will cause it
        to wait forever.
        """
        pids = ru.as_list(pids)
        data = {'pids'   : pids,
                'states' : ru.as_list(states),
                'timeout': timeout}

        args = ['post', '/sessions/%s/pilots/' % sid, data]
        if pids and len(pids) == 1 and pids[0]:
            args[1] += '%s/' % pids[0]

        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def pilots_cancel(self, sid, pids=None):
        """
        cancel all resources (ie. RP pilots) with the given UIDs (or for all
        known resources if no UID is specified).  This call will return when the
        resource states are final.
        """
        pids = ru.as_list(pids)

        args = ['delete', '/sessions/%s/pilots/' % sid]
        if pids and len(pids) == 1 and pids[0]:
            args[1] += '%s/' % pids[0]
        else:
            args.append({'pids': pids})

        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def tasks_submit(self, sid, descriptions):
        """
        task descriptions are submitted to the RP level resources  (pilots).
        """
        if not descriptions:
            return []

        args = ['put', '/sessions/%s/tasks/' % sid, ru.as_list(descriptions)]
        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def tasks_inspect(self, sid, tids=None):
        """
        return UIDs for all known tasks
        """
        tids = ru.as_list(tids)

        args = ['get', '/sessions/%s/tasks/' % sid]
        if tids and len(tids) == 1 and tids[0]:
            args[1] += '%s/' % tids[0]
        elif tids:
            args.append({'tids': tids})

        return self._query(*args)

    # --------------------------------------------------------------------------
    #
    def tasks_stdout(self, sid, tid):
        """
        return the stdout of a completed task
        """
        return self._query('get', '/sessions/%s/tasks/%s/stdout' % (sid, tid))

    # --------------------------------------------------------------------------
    #
    def tasks_stderr(self, sid, tid):
        """
        return the stderr of a completed task
        """
        return self._query('get', '/sessions/%s/tasks/%s/stderr' % (sid, tid))

    # --------------------------------------------------------------------------
    #
    def tasks_wait(self, sid, tids=None, states=None, timeout=None):
        """
        wait for a specific (set of) states for all tasks
        with the given UIDs (or for all known tasks if no UID is specified).
        This call will return after a given timeout, or after the states have
        been reached, whichever occurs first.  A negative timeout value will
        cause it to wait forever.
        """
        tids = ru.as_list(tids)
        data = {'tids'   : tids,
                'states' : ru.as_list(states),
                'timeout': timeout}

        args = ['post', '/sessions/%s/tasks/' % sid, data]
        if tids and len(tids) == 1 and tids[0]:
            args[1] += '%s/' % tids[0]

        return self._query(*args)

# ------------------------------------------------------------------------------

