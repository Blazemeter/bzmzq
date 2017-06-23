import json
import time
from uuid import uuid4

from helpers import cached_prop
from states import JobStates


class Job(object):
    STATIC_PROPS = ['created', 'started', 'finished', 'name', 'module', 'module_kwargs', 'description', 'result',
                    'worker']
    DYNAMIC_PROPS = ['state']
    ALLOWED_PROPS = STATIC_PROPS + DYNAMIC_PROPS

    DEFAULT_PRIORITY = 100

    def __init__(self, queue, job_id, priority=DEFAULT_PRIORITY):
        self._queue = queue
        self._job_id = job_id
        self._priority = priority

    @classmethod
    def create(cls, queue, name, module, module_kwargs=None, priority=DEFAULT_PRIORITY):
        job_id = str(uuid4())
        job_path = queue.path_factory.job.id(job_id)
        queue._kz_ses.ensure_path(str(job_path))

        if module_kwargs is not None and not isinstance(module_kwargs, dict):
            raise ValueError("module_kwargs can be a dict or None")

        for prop in cls.STATIC_PROPS:
            prop_path = queue.path_factory.job.prop(job_id, prop)
            queue._kz_ses.ensure_path(str(prop_path))

        job_instance = cls(queue, job_id, priority)
        job_instance.created = time.time()
        job_instance.name = name
        job_instance.module = module
        job_instance.module_kwargs = module_kwargs if module_kwargs else {}
        job_instance.state = JobStates.STATE_PENDING

        return job_instance

    @cached_prop
    def id(self):
        return self._job_id

    def _set_prop(self, prop, val):
        if prop not in self.ALLOWED_PROPS:
            raise ValueError("Prop [{}] is not in allowed prop list".format(prop))

        prop_path = self._queue.path_factory.job.prop(self.id, prop)
        if self._get_prop(prop):
            raise RuntimeError("You can not change props after they were set")
        self._queue._kz_ses.set(str(prop_path), json.dumps(val))

    def _get_prop(self, prop):
        prop_path = self._queue.path_factory.job.prop(self.id, prop)
        val, _ = self._queue._kz_ses.get(str(prop_path))
        return None if val == '' else json.loads(val)

    def _reset_state(self):
        for state_name, state_id in JobStates().iteritems():
            state_path = self._queue.path_factory.job.state(self.id, state_id)
            self._queue._kz_ses.delete(str(state_path), recursive=True)

    def _set_state(self, state_id):
        if state_id not in JobStates().values():
            raise ValueError("State [{}] is unknown".format(state_id))
        self._reset_state()
        state_path = self._queue.path_factory.job.state(self.id, state_id)
        self._queue._kz_ses.ensure_path(str(state_path))
        if state_id == JobStates.STATE_PENDING:
            self._queue._kz_queue.put(self._job_id, self._priority)

    def _get_state(self):
        for state_name, state_id in JobStates().iteritems():
            state_path = self._queue.path_factory.job.state(self.id, state_id)
            if self._queue._kz_ses.exists(str(state_path)):
                return state_name, state_id
        raise RuntimeError("Job state could not be determined")

    def delete(self):
        job_path = self._queue.path_factory.job.id(self.id)
        self._reset_state()
        self._queue._kz_ses.delete(str(job_path), recursive=True)

    def __getattr__(self, prop):
        if prop == 'state':
            return self._get_state()
        if prop in self.ALLOWED_PROPS:
            return self._get_prop(prop)
        raise AttributeError("Could not find prop [{}]".format(prop))

    def __setattr__(self, name, value):
        if name == 'state':
            self._set_state(value)
        elif name not in self.ALLOWED_PROPS:
            return super(Job, self).__setattr__(name, value)
        else:
            self._set_prop(name, value)
