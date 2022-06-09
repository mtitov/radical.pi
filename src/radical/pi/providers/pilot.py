
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
class PilotClient:
    """Interface class from `Service` like API to `radical.pilot`.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, log=None, prof=None, rep=None):

        ns = self.__class__.__name__.lower()

        if log : self._log  = log
        else   : self._log  = ru.Logger(ns)

        if prof: self._prof = prof
        else   : self._prof = ru.Profiler(ns)

        if rep : self._rep  = log
        else   : self._rep  = ru.Reporter(ns)

        self._pmgr = None
        self._tmgr = None

        self._session = rp.Session()
        self._init_pilot_manager()

        # create a dir for data staging
        self._work_dir = os.getcwd()
        self._data_dir = 'data.%s' % self._session.uid

        # track submitted tasks
        self._tasks      = {}

    # --------------------------------------------------------------------------
    #
    def _init_pilot_manager(self):

        if self._pmgr is None:
            self._pmgr = rp.PilotManager(self._session)
            self._pmgr.register_callback(self._pilot_state_cb)

    # --------------------------------------------------------------------------
    #
    def _init_task_manager(self):

        if self._tmgr is None:
            self._tmgr = rp.TaskManager(self._session)
            self._tmgr.register_callback(self._task_state_cb)

    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):

        return self._session.uid

    # --------------------------------------------------------------------------
    #
    def close(self):

        self._session.close(download=True)

    # --------------------------------------------------------------------------
    #
    def submit(self, requests):

        self._rep.info('\nrequesting dedicated pilots\n')

        pilot_descr = []
        for request in requests:
            pilot_descr.append(rp.PilotDescription(dict(request)))

        pilots = self._pmgr.submit_pilots(pilot_descr)
        return [p.uid for p in pilots]

    # --------------------------------------------------------------------------
    #
    def inspect(self, pids=None):

        self._rep.info('\nget pilot info: %s\n' % pids or 'ALL')

        output = []
        pilots = ru.as_list(self._pmgr.get_pilots(uids=pids))

        for pilot in pilots:
            self._rep.ok('    %s\n' % pilot.uid)
            output.append(pilot.as_dict())

        return output

    # --------------------------------------------------------------------------
    #
    def wait(self, pids=None, states=None, timeout=None):

        self._rep.info('\nwait for pilots: %s (%s) (%s)\n' %
                       (pids or 'ALL', states, timeout))

        return self._pmgr.wait_pilots(uids=pids, state=states, timeout=timeout)

    # --------------------------------------------------------------------------
    #
    def cancel(self, pids=None):

        self._rep.info('\ncancel pilots: %s\n' % pids or 'ALL')

        self._pmgr.cancel_pilots(pids)
        self._pmgr.wait_pilots(pids, rp.FINAL)

        states = list()
        for pilot in self._pmgr.get_pilots(pids):
            self._rep.ok('%s: %10s\n' % (pilot.uid, pilot.state))
            states.append(pilot.state)

        return states

    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):

        if self._tmgr is None:
            self._init_task_manager()
            if self._pmgr:
                self._tmgr.add_pilots(self._pmgr.get_pilots())

            ru.rec_makedir(os.path.join(self._work_dir, self._data_dir))

        self._rep.header('submit tasks\n')

        tds = []
        for descr in descriptions:
            stdout = descr.setdefault('stdout', 'STDOUT')
            stderr = descr.setdefault('stderr', 'STDERR')
            descr.setdefault('output_staging', []).extend(
                [{'source': 'task:///%s' % stdout,
                  'target': 'client:///%s/${RP_TASK_ID}.out' % self._data_dir,
                  'action': rp.TRANSFER},
                 {'source': 'task:///%s' % stderr,
                  'target': 'client:///%s/${RP_TASK_ID}.err' % self._data_dir,
                  'action': rp.TRANSFER}])
            tds.append(rp.TaskDescription(descr))

        tasks = self._tmgr.submit_tasks(tds)
        for t in tasks:
            self._tasks[t.uid] = t

        return [t.uid for t in tasks]

    # --------------------------------------------------------------------------
    #
    def _pilot_state_cb(self, pilot, state):

        if state in rp.FINAL:
            self._rep.ok('pilot completed %s: %s\n' % (pilot.uid, pilot.state))
            if self._tmgr:
                self._tmgr.remove_pilots(pilot.uid)

        return True

    # --------------------------------------------------------------------------
    #
    def _task_state_cb(self, task, state):

        if state == rp.DONE:
            self._rep.ok('task completed %s\n' % task.uid)
        elif state == rp.FAILED:
            self._rep.error('task failed    %s\n' % task.uid)

        return True

    # --------------------------------------------------------------------------
    #
    def inspect_tasks(self, tids=None):

        self._rep.info('\nget task info: %s\n' % tids or 'ALL')

        output = []
        tasks = ru.as_list(self._tmgr.get_tasks(uids=tids))

        for task in tasks:
            self._rep.ok('    %s\n' % task.uid)
            output.append(task.as_dict())

        return output

    # --------------------------------------------------------------------------
    #
    def _get_task_output(self, tid, ftype):

        if ftype not in ['out', 'err']:
            raise RuntimeError('task output format incorrect: %s' % ftype)

        self._rep.info('\nget task std%s: %s\n' % (ftype, tid))

        if tid not in self._tasks:
            raise ValueError('task ID is unknown')

        std_fname = os.path.join(self._work_dir,
                                 self._data_dir,
                                 '%s.%s' % (tid, ftype))

        if not os.path.isfile(std_fname):
            raise RuntimeError('std%s for %s is not available' % (ftype, tid))

        with open(std_fname, 'r') as fd:
            output = fd.read()
        return output

    # --------------------------------------------------------------------------
    #
    def tasks_stdout(self, tid):
        return self._get_task_output(tid=tid, ftype='out')

    # --------------------------------------------------------------------------
    #
    def tasks_stderr(self, tid):
        return self._get_task_output(tid=tid, ftype='err')

    # --------------------------------------------------------------------------
    #
    def wait_tasks(self, tids=None, states=None, timeout=None):

        self._rep.info('\nwait for tasks: %s (%s)\n' % (tids or 'ALL', states))
        task_states = self._tmgr.wait_tasks(uids=tids,
                                            state=states,
                                            timeout=timeout)

        self._tmgr.close()
        return task_states

# ------------------------------------------------------------------------------

