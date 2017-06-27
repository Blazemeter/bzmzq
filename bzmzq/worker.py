import argparse
import importlib
import inspect
import platform
import sys
import traceback

import os
from kazoo.client import KazooState
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
        return '{0}-{1}-{2}'.format(self.__class__.__name__,
                                    platform.node(), os.getpid())

    def __state_handler(self, state):
        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._logger.critical("Lost connection to ZooKeeper, exiting...")
            os._exit(1)

    def _register_worker(self):
        self._logger.info("Registering worker")
        my_worker_path = str(self._queue.path_factory.worker.id(self.id))

        try:
            self._queue.kz_ses.create(path=my_worker_path, ephemeral=True)
        except NodeExistsError:
            pass

    def _import_class(self, module_name):
        imported_module = importlib.import_module(module_name)
        reload(imported_module)
        for cls_name, cls_obj in inspect.getmembers(
                imported_module, inspect.isclass):
            if cls_obj.__name__ != IJobWorker.__name__ and IJobWorker.__name__ in [base_cls.__name__ for base_cls in
                                                                                   cls_obj.__bases__]:
                return cls_obj
        raise ImportError("Could not find a class implementing IJobWorker")

    def _handle_job(self, job_id):
        job = Job(self._queue, job_id)
        job.worker = self.id
        job.state = JobStates.STATE_RUNNING

        try:
            found_cls = self._import_class(job.module)
            inst = found_cls(self._queue, job, **job.module_kwargs)
            try:
                job.result = inst.run()
            finally:
                inst.teardown()
        except BaseException:
            job.result = traceback.format_exc()
            job.state = JobStates.STATE_FAILED
        else:
            job.state = JobStates.STATE_SUCCESS

    def run(self, run_once=False):
        self._queue.kz_ses.add_listener(self.__state_handler)
        self._register_worker()
        while True:
            try:
                job_id = self._queue._kz_queue.get()
                self._queue._kz_queue.consume()
                self._logger.info("Handling job {}".format(job_id))
                self._handle_job(job_id)
            except BaseException:
                self._logger.error(traceback.format_exc())
            finally:
                if run_once:
                    exit(0)


def main():
    parser = argparse.ArgumentParser(description='BzmZQ Worker')
    parser.add_argument(
        '-r',
        '--run-once',
        type=bool,
        default=False,
        help='Should worker exit after one job.')
    parser.add_argument(
        '-z',
        '--zkservers',
        type=str,
        required=True,
        help='Zookeeper servers. "127.0.0.1:2181,127.0.0.1:2182"')
    parser.add_argument(
        '-q',
        '--queue',
        type=str,
        required=True,
        help='Queue name')
    parser.add_argument(
        '-m',
        '--module-path',
        type=str,
        required=False,
        nargs='*',
        help='Module paths')

    args = parser.parse_args()
    for path in set(args.module_path):
        sys.path.insert(0, path)
    q = Queue(args.zkservers, args.queue)

    w = WorkListener(q)
    w.run(args.run_once)


if __name__ == "__main__":
    main()
