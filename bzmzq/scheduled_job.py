import json

from .helpers import cached_prop
from .states import JobStates, ScheduledJobStates


class ScheduledJob(object):
    # Write once props
    WO_STATIC_PROPS = [
        'id',
        'name',
        'module',
        'module_kwargs',
        'interval_sec',
        'concurrent',
        'next_run',
        'last_job_id',
        'priority']
    # Write always props
    WA_STATIC_PROPS = ['next_run', 'last_job_id']
    DYNAMIC_PROPS = ['state']
    ALLOWED_PROPS = WO_STATIC_PROPS + DYNAMIC_PROPS

    DEFAULT_PRIORITY = 100
    DEFAULT_INTERVAL = 60

    MINIMUM_INTERVAL_SEC = 3

    BAD_CHARS = ['/']

    def __init__(self, queue, scheduled_job_id):
        self._queue = queue
        self._scheduled_job_id = scheduled_job_id

    @classmethod
    def create(
            cls, queue, scheduled_job_id, name, module, module_kwargs=None,
            priority=DEFAULT_PRIORITY, interval_sec=DEFAULT_INTERVAL,
            concurrent=True, override=False):

        for c in cls.BAD_CHARS:
            if c in scheduled_job_id:
                raise ValueError("Char {} not allowed in scheduled_job_id".format(c))

        if interval_sec < cls.MINIMUM_INTERVAL_SEC:
            raise ValueError("Minmum interval for a scheudled job is {}".format(cls.MINIMUM_INTERVAL_SEC))

        scheduled_job_path = str(queue.path_factory.scheduled_job.id(scheduled_job_id))
        if queue.kz_ses.exists(scheduled_job_path):
            if not override:
                raise RuntimeError(
                    "Scheduled job already exists and override set to false")
            cls(queue, scheduled_job_id).delete()

        queue.kz_ses.ensure_path(scheduled_job_path)
        queue.kz_ses.sync(scheduled_job_path)

        if module_kwargs is not None and not isinstance(module_kwargs, dict):
            raise ValueError("module_kwargs can be a dict or None")

        for prop in cls.WO_STATIC_PROPS:
            prop_path = str(queue.path_factory.scheduled_job.prop(scheduled_job_id, prop))
            queue.kz_ses.ensure_path(prop_path)
            queue.kz_ses.sync(prop_path)

        sj = cls(queue, scheduled_job_id)
        sj.name = name
        sj.module = module
        sj.module_kwargs = module_kwargs if module_kwargs else {}
        sj.interval_sec = interval_sec
        sj.concurrent = concurrent
        sj.priority = priority
        sj.state = ScheduledJobStates.STATE_ENABLED

        return sj

    @cached_prop
    def id(self):
        return self._scheduled_job_id

    def _set_prop(self, prop, val):
        if prop not in self.ALLOWED_PROPS:
            raise ValueError(
                "Prop [{}] is not in allowed prop list".format(prop))

        prop_path = str(self._queue.path_factory.scheduled_job.prop(self.id, prop))
        self._queue.kz_ses.set(prop_path, json.dumps(val))
        self._queue.kz_ses.sync(prop_path)

    def _get_prop(self, prop):
        prop_path = str(self._queue.path_factory.scheduled_job.prop(self.id, prop))
        self._queue.kz_ses.sync(prop_path)
        val, _ = self._queue.kz_ses.get(prop_path)
        return None if val == '' else json.loads(val)

    def _reset_state(self):
        for state_name, state_id in ScheduledJobStates().items():
            state_path = str(self._queue.path_factory.scheduled_job.state(self.id, state_id))
            self._queue.kz_ses.delete(state_path, recursive=True)
            self._queue.kz_ses.sync(state_path)

    def _set_state(self, state_id):
        if state_id not in ScheduledJobStates().values():
            raise ValueError("State [{}] is unknown".format(state_id))
        self._reset_state()
        state_path = str(self._queue.path_factory.scheduled_job.state(self.id, state_id))
        self._queue.kz_ses.ensure_path(state_path)
        self._queue.kz_ses.sync(state_path)

    def _get_state(self):
        for state_name, state_id in JobStates().items():
            state_path = self._queue.path_factory.scheduled_job.state(
                self.id, state_id)
            if self._queue.kz_ses.exists(str(state_path)):
                return state_name, state_id
        raise RuntimeError("Scheduled Job state could not be determined")

    def delete(self):
        scheduled_job_path = str(self._queue.path_factory.scheduled_job.id(self.id))
        self._reset_state()
        self._queue.kz_ses.delete(scheduled_job_path, recursive=True)
        self._queue.kz_ses.sync(scheduled_job_path)

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
        elif name not in self.WA_STATIC_PROPS and self._get_prop(name):
            raise RuntimeError("You can not change props after they were set")
        else:
            self._set_prop(name, value)
