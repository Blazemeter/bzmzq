import argparse
import importlib
import inspect
import platform
import traceback

import os
from kazoo.exceptions import NodeExistsError

from helpers import cached_prop, get_logger
from ijobworker import IJobWorker
from job import Job
from queue import Queue
from states import JobStates


class WorkListener(object):
    def __init__(self, queue):
        self._queue = queue
        self._logger = get_logger(self.id)

    @cached_prop
    def id(self):
        return '{0}-{1}-{2}'.format(self.__class__.__name__, platform.node(), os.getpid())

    def _register_worker(self):
        self._logger.info("Registering worker")
        my_worker_path = str(self._queue.path_factory.worker.id(self.id))

        try:
            self._queue._kz_ses.create(path=my_worker_path, ephemeral=True)
        except NodeExistsError:
            pass

    def _import_class(self, module_name):
        imported_module = importlib.import_module(module_name)

        for cls_name, cls in inspect.getmembers(imported_module, inspect.isclass):
            if cls != IJobWorker and issubclass(cls, IJobWorker):
                return cls
        raise ImportError("Could not find a class implementing IJobWorker")

    def _handle_job(self, job_id):
        job = Job(self._queue, job_id)
        job.worker = self.id
        job.state = JobStates.STATE_RUNNING

        try:
            found_cls = self._import_class(job.module)
            inst = found_cls()
            inst.setup(self._queue, **job.module_kwargs)
            job.result = inst.run()
        except:
            job.result = traceback.format_exc()
            job.state = JobStates.STATE_FAILED
        else:
            job.state = JobStates.STATE_SUCCESS

    def run(self):
        self._register_worker()
        while 1:
            try:
                job_id = self._queue._kz_queue.get()
                self._queue._kz_queue.consume()
                self._logger.info("Handling job {}".format(job_id))
                self._handle_job(job_id)
            except:
                print traceback.format_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='BzmZQ Worker')
    parser.add_argument('-z', '--zkservers', type=str, required=True,
                        help='Zookeeper servers. "127.0.0.1:2181,127.0.0.1:2182"')
    parser.add_argument('-q', '--queue', type=str, required=True, help='Queue name')

    args = parser.parse_args()
    q = Queue(args.zkservers, args.queue)

    s = WorkListener(q)
    s.run()
