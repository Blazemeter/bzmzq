import json
import time
from uuid import uuid4

import custom_exceptions as exceptions
from helpers import cached_prop
from states import JobStates


class Job(object):
    # Write once props
    WO_STATIC_PROPS = [
        'created',
        'started',
        'ended',
        'name',
        'module',
        'module_kwargs',
        'description',
        'result',
        'error',
        'worker',
        'parent_job_id',
        'log'
    ]

    DYNAMIC_PROPS = ['state']
    ALLOWED_PROPS = WO_STATIC_PROPS + DYNAMIC_PROPS

    DEFAULT_PRIORITY = 100

    def __init__(self, queue, job_id, priority=DEFAULT_PRIORITY):
        self._queue = queue
        self._job_id = job_id
        self._priority = priority

    @classmethod
    def create(cls, queue, module, name=None, module_kwargs=None, parent_job_id=None,
               priority=DEFAULT_PRIORITY):
        job_id = str(uuid4())
        job_path = str(queue.path_factory.job.id(job_id))
        queue.kz_ses.ensure_path(job_path)
        queue.kz_ses.sync(job_path)

        if module_kwargs is not None and not isinstance(module_kwargs, dict):
            raise ValueError("module_kwargs can be a dict or None")

        for prop in cls.ALLOWED_PROPS:
            prop_path = str(queue.path_factory.job.prop(job_id, prop))
            queue.kz_ses.ensure_path(prop_path)
            queue.kz_ses.sync(prop_path)

        job_instance = cls(queue, job_id, priority)
        job_instance.created = time.time()
        job_instance.name = name
        job_instance.module = module
        job_instance.module_kwargs = module_kwargs if module_kwargs else {}
        job_instance.parent_job_id = parent_job_id
        job_instance.state = JobStates.STATE_PENDING

        return job_instance

    @cached_prop
    def id(self):
        return self._job_id

    def _set_prop(self, prop, val):
        if prop not in self.ALLOWED_PROPS:
            raise ValueError(
                "Prop [{}] is not in allowed prop list".format(prop))

        prop_path = str(self._queue.path_factory.job.prop(self.id, prop))

        if self._get_prop(prop) and prop in self.WO_STATIC_PROPS:
            raise RuntimeError("You can not change props after they were set")

        self._queue.kz_ses.set(prop_path, json.dumps(val))
        self._queue.kz_ses.sync(prop_path)

    def _get_prop(self, prop):
        prop_path = str(self._queue.path_factory.job.prop(self.id, prop))
        self._queue.kz_ses.sync(prop_path)
        val, _ = self._queue.kz_ses.get(prop_path)
        return None if val == '' else json.loads(val)

    def _reset_state(self):
        for state_name, state_id in JobStates().iteritems():
            state_path = str(self._queue.path_factory.job.state(self.id, state_id))
            self._queue.kz_ses.delete(state_path, recursive=True)
            self._queue.kz_ses.sync(state_path)

    def _set_state(self, state_id):
        if state_id not in JobStates().values():
            raise ValueError("State [{}] is unknown".format(state_id))
        self._reset_state()
        if state_id == JobStates.STATE_PENDING:
            self._queue._kz_queue.put(self._job_id, self._priority)
        state_path = str(self._queue.path_factory.job.state(self.id, state_id))
        self._queue.kz_ses.ensure_path(state_path)
        self._queue.kz_ses.sync(state_path)

    def _get_state(self):
        state_id = self._get_prop('state')
        state_name = {k:v for k, v in JobStates().iteritems() if v == state_id}

        if not state_name:
            raise exceptions.UnknownJobState("Job state could not be determined")

        return state_name.values()[0], state_id


    def wait(self, raise_on_error=True, timeout_sec=60):
        WATCH_INTERVAL_SEC = 1

        expiry = time.time() + timeout_sec
        try:
            while self.state[1] not in [JobStates.STATE_FAILED, JobStates.STATE_SUCCESS]:
                if time.time() > expiry:
                    raise exceptions.TimeoutError("Job wait timed out.")
                time.sleep(WATCH_INTERVAL_SEC)

            if raise_on_error and self.state[1] == JobStates.STATE_FAILED:
                raise exceptions.JobException(self.error)

            return self.result
        except exceptions.UnknownJobState:
            pass

    def delete(self):
        job_path = str(self._queue.path_factory.job.id(self.id))
        self._reset_state()
        self._queue.kz_ses.delete(job_path, recursive=True)
        self._queue.kz_ses.sync(job_path)

    def __getattr__(self, prop):
        if prop == 'state':
            return self._get_state()
        if prop in self.ALLOWED_PROPS:
            return self._get_prop(prop)
        raise AttributeError("Could not find prop [{}]".format(prop))

    def __setattr__(self, name, value):
        if name not in self.ALLOWED_PROPS:
            return super(Job, self).__setattr__(name, value)
        else:
            self._set_prop(name, value)
            if name == 'state' and value == JobStates.STATE_PENDING:
                self._queue._kz_queue.put(self._job_id, self._priority)

