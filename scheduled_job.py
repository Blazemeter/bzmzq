import json

from helpers import cached_prop
from states import JobStates, ScheduledJobStates


class ScheduledJob(object):
    STATIC_PROPS = ['id', 'name', 'module', 'module_kwargs', 'interval_min', 'concurrent', 'next_run', 'last_job_id',
                    'priority']
    OVERRIDABLE_STATIC_PROPS = ['next_run', 'last_job_id']
    DYNAMIC_PROPS = ['state']
    ALLOWED_PROPS = STATIC_PROPS + DYNAMIC_PROPS

    DEFAULT_PRIORITY = 100
    DEFAULT_INTERVAL = 1

    def __init__(self, queue, scheduled_job_id):
        self._queue = queue
        self._scheduled_job_id = scheduled_job_id

    @classmethod
    def create(cls, queue, scheduled_job_id, name, module, module_kwargs=None, priority=DEFAULT_PRIORITY,
               interval_min=DEFAULT_INTERVAL, concurrent=True, override=False):

        scheduled_job_path = queue.path_factory.scheduled_job.id(scheduled_job_id)
        if queue._kz_ses.exists(str(scheduled_job_path)):
            if not override:
                raise RuntimeError("Scheduled job already exists and override set to false")
            cls(queue, scheduled_job_id).delete()

        queue._kz_ses.ensure_path(str(scheduled_job_path))

        if module_kwargs is not None and not isinstance(module_kwargs, dict):
            raise ValueError("module_kwargs can be a dict or None")

        for prop in cls.STATIC_PROPS:
            prop_path = queue.path_factory.scheduled_job.prop(scheduled_job_id, prop)
            queue._kz_ses.ensure_path(str(prop_path))

        scheduled_job_instance = cls(queue, scheduled_job_id)
        scheduled_job_instance.name = name
        scheduled_job_instance.module = module
        scheduled_job_instance.module_kwargs = module_kwargs if module_kwargs else {}
        scheduled_job_instance.interval_min = interval_min
        scheduled_job_instance.concurrent = concurrent
        scheduled_job_instance.priority = priority
        scheduled_job_instance.state = ScheduledJobStates.STATE_ENABLED

        return scheduled_job_instance

    @cached_prop
    def id(self):
        return self._scheduled_job_id

    def _set_prop(self, prop, val):
        if prop not in self.ALLOWED_PROPS:
            raise ValueError("Prop [{}] is not in allowed prop list".format(prop))

        prop_path = self._queue.path_factory.scheduled_job.prop(self.id, prop)
        self._queue._kz_ses.set(str(prop_path), json.dumps(val))

    def _get_prop(self, prop):
        prop_path = self._queue.path_factory.scheduled_job.prop(self.id, prop)
        val, _ = self._queue._kz_ses.get(str(prop_path))
        return None if val == '' else json.loads(val)

    def _reset_state(self):
        for state_name, state_id in ScheduledJobStates().iteritems():
            state_path = self._queue.path_factory.scheduled_job.state(self.id, state_id)
            self._queue._kz_ses.delete(str(state_path), recursive=True)

    def _set_state(self, state_id):
        if state_id not in ScheduledJobStates().values():
            raise ValueError("State [{}] is unknown".format(state_id))
        self._reset_state()
        state_path = self._queue.path_factory.scheduled_job.state(self.id, state_id)
        self._queue._kz_ses.ensure_path(str(state_path))

    def _get_state(self):
        for state_name, state_id in JobStates().iteritems():
            state_path = self._queue.path_factory.scheduled_job.state(self.id, state_id)
            if self._queue._kz_ses.exists(str(state_path)):
                return state_name, state_id
        raise RuntimeError("Scheduled Job state could not be determined")

    def delete(self):
        scheduled_job_path = self._queue.path_factory.scheduled_job.id(self.id)
        self._reset_state()
        self._queue._kz_ses.delete(str(scheduled_job_path), recursive=True)

    def __getattr__(self, prop):
        if prop not in self.ALLOWED_PROPS:
            raise AttributeError("Could not find prop [{}]".format(prop))
        elif prop == 'state':
            return self._get_state()
        else:
            return self._get_prop(prop)

    def __setattr__(self, name, value):
        if name == 'state':
            self._set_state(value)
        elif name not in self.ALLOWED_PROPS:
            return super(ScheduledJob, self).__setattr__(name, value)
        elif name not in self.OVERRIDABLE_STATIC_PROPS and self._get_prop(name):
            raise RuntimeError("You can not change props after they were set")
        else:
            self._set_prop(name, value)
