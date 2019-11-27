from abc import ABCMeta, abstractmethod
from functools import wraps

from .helpers import ZkPath, cached_prop


def no_tree_traversal(func):
    @wraps(func)
    def __wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    __wrapper.__no_traversal__ = True
    return __wrapper


class _PathBase(object):
    __metaclass__ = ABCMeta

    SEP = '/'
    ROOT_METHOD = 'root'

    def __init__(self, path_factory, parent=None):
        self._path_factory = path_factory
        self._parent = parent

    @abstractmethod
    def root(self):
        return []

    def _absoluteattr(self, item):
        return super(_PathBase, self).__getattribute__(item)

    def __getattribute__(self, item):
        attr = super(_PathBase, self).__getattribute__(item)
        if not callable(attr) or item.startswith(
                '_') or hasattr(attr, '__no_traversal__'):
            return attr

        def _traversal_wrapper(*args, **kwargs):
            my_root = self._absoluteattr(self.ROOT_METHOD)()
            parent_root = [] if not self._parent else getattr(
                self._parent, self.ROOT_METHOD)()
            final_path = parent_root + my_root

            # If we are getting other method than ROOT_METHOD, get it too.
            if item != self.ROOT_METHOD:
                final_path += self._absoluteattr(item)(*args, **kwargs)
            return ZkPath(final_path)

        return _traversal_wrapper


class _QueuePath(_PathBase):
    def root(self):
        return [self._path_factory.queue_instance.ZK_TREE_ROOT,
                self._path_factory.queue_instance.queue_name]

    def kz_queue(self):
        return ['kz_queue']


class _JobPath(_PathBase):
    def root(self):
        return ['jobs']

    def id(self, job_id):
        return [job_id]

    def prop(self, job_id, prop):
        return [job_id, prop]

    @no_tree_traversal
    def state(self, job_id, state_id):
        return self._path_factory.job_state.id(state_id) + [job_id]


class _JobStatePath(_PathBase):
    def root(self):
        return ['job_states']

    def id(self, state_id):
        return [state_id]


class _ScheduledJobStatePath(_JobStatePath):
    def root(self):
        return ['scheduled_job_states']


class _LockPath(_PathBase):
    def root(self):
        return ['locks']

    def name(self, lock_name):
        return [lock_name]


class _WorkerPath(_PathBase):
    def root(self):
        return ['workers']

    def id(self, worker_id):
        return [worker_id]


class _ScheduledJobPath(_PathBase):
    def root(self):
        return ['scheduled_jobs']

    def id(self, scheduled_job_id):
        return [scheduled_job_id]

    def prop(self, scheduled_job_id, prop):
        return [scheduled_job_id, prop]

    @no_tree_traversal
    def state(self, scheduled_job_id, state_id):
        return self._path_factory.scheduled_job_state.id(
            state_id) + [scheduled_job_id]


class PathFactory(object):
    def __init__(self, queue):
        self.queue_instance = queue

    @cached_prop
    def queue(self):
        return _QueuePath(self)

    @cached_prop
    def job(self):
        return _JobPath(self, self.queue)

    @cached_prop
    def job_state(self):
        return _JobStatePath(self, self.queue)

    @cached_prop
    def scheduled_job(self):
        return _ScheduledJobPath(self, self.queue)

    @cached_prop
    def scheduled_job_state(self):
        return _ScheduledJobStatePath(self, self.queue)

    @cached_prop
    def lock(self):
        return _LockPath(self, self.queue)

    @cached_prop
    def worker(self):
        return _WorkerPath(self, self.queue)

    def is_tree_item(self, obj):
        return isinstance(obj, _PathBase)

    def get_path_roots(self):
        paths = []
        for attr_name in dir(self):
            tree_item = getattr(self, attr_name)
            if self.is_tree_item(tree_item):
                tree_item_root = [getattr(tree_item, root_fn)()
                                  for root_fn in dir(tree_item)
                                  if root_fn.startswith(
                        _PathBase.ROOT_METHOD)]
                paths += tree_item_root
        return paths
