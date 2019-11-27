from threading import Lock as TLock
from weakref import WeakValueDictionary

from kazoo.client import KazooClient

from .helpers import cached_prop
from .job import Job
from .path import PathFactory
from .scheduled_job import ScheduledJob
# from scheduler import Scheduler
from .states import JobStates, ScheduledJobStates


class Queue(object):
    ZK_TREE_ROOT = 'bzmzq'
    ZK_QUEUE_LOCK_NAME = 'main-lock'

    def __init__(self, zk_servers, queue_name):
        self._queue_name = queue_name
        self.kz_ses = KazooClient(zk_servers)
        self.kz_ses.start()

        self.servers = zk_servers

        self._kz_queue = self.kz_ses.LockingQueue(
            str(self.path_factory.queue.kz_queue()))

        self._tlock = TLock()
        self._rlock_cache = WeakValueDictionary()  # {<rlock_name>: RLock}

        self._make_paths()

    @cached_prop
    def path_factory(self):
        return PathFactory(self)

    @cached_prop
    def queue_name(self):
        return self._queue_name

    def _make_paths(self):
        for root_path in self.path_factory.get_path_roots():
            root_path = str(root_path)
            self.kz_ses.ensure_path(str(root_path))
            self.kz_ses.sync(root_path)

        for state_id in list(JobStates().values()):
            state_path = str(self.path_factory.job_state.id(state_id))
            self.kz_ses.ensure_path(str(state_path))
            self.kz_ses.sync(state_path)

        for state_id in list(ScheduledJobStates().values()):
            state_path = str(self.path_factory.scheduled_job_state.id(state_id))
            self.kz_ses.ensure_path(str(state_path))
            self.kz_ses.sync(state_path)

    def get_lock(self, lock_name=None):
        with self._tlock:
            lock_name = lock_name if lock_name else self.ZK_QUEUE_LOCK_NAME
            cached_lock = self._rlock_cache.get(lock_name)

            if cached_lock:
                return cached_lock

            lock_path = self.path_factory.lock.name(lock_name)
            new_lock = self.kz_ses.Lock(str(lock_path))
            self._rlock_cache[lock_name] = new_lock
            return new_lock

    def create_job(self, *args, **kwargs):
        return Job.create(self, *args, **kwargs)

    def create_scheduled_job(self, *args, **kwargs):
        return ScheduledJob.create(self, *args, **kwargs)

    def get_jobs(self, state=None):
        if state is None:
            path = str(self.path_factory.job.root())
        else:
            if state not in list(JobStates().values()):
                raise ValueError("Unknown job state")
            path = str(self.path_factory.job_state.id(state))

        return [Job(self, job_id)
                for job_id in self.kz_ses.get_children(path)]

    def get_scheduled_jobs(self, state=None):
        if state is None:
            path = str(self.path_factory.scheduled_job.root())
        else:
            if state not in list(ScheduledJobStates().values()):
                raise ValueError("Unknown scheduled job state")
            path = str(self.path_factory.scheduled_job_state.id(state))
        return [ScheduledJob(self, scheduled_job_id)
                for scheduled_job_id in self.kz_ses.get_children(path)]
