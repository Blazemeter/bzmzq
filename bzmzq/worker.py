import argparse
import importlib
import inspect
import logging
import platform
import signal
import sys
import time
import traceback
from cStringIO import StringIO as StringBuffer

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

        signal.signal(signal.SIGINT, self.__signal_handler)

    @cached_prop
    def id(self):
        return '{0}-{1}-{2}'.format(self.__class__.__name__,
                                    platform.node(), os.getpid())

    def __state_handler(self, state):
        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._logger.critical("Lost connection to ZooKeeper, exiting...")
            sys.exit(1)

    def __signal_handler(self, signal, frame):
        sys.exit(1)

    def _register_worker(self):
        self._logger.info("Registering worker")
        my_worker_path = str(self._queue.path_factory.worker.id(self.id))

        try:
            self._queue.kz_ses.create(path=my_worker_path, ephemeral=True)
            self._queue.kz_ses.sync(my_worker_path)
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

    def _get_logger_for_job(self, job_id):
        logger = logging.getLogger('job.' + job_id)
        logger.setLevel(logging.DEBUG)

        log_capture_string = StringBuffer()
        ch = logging.StreamHandler(log_capture_string)
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        logger.addHandler(ch)

        return logger, log_capture_string

    def _handle_job(self, job_id):
        job = Job(self._queue, job_id)
        job.worker = self.id
        job.state = JobStates.STATE_RUNNING
        job.started = time.time()
        self._logger.info(
            "Running job params are [{name}] [{module}] [{module_kwargs}]".format(name=job.name, module=job.module,
                                                                                  module_kwargs=job.module_kwargs))

        job_logger, job_log_buffer = self._get_logger_for_job(job_id)
        try:

            found_cls = self._import_class(job.module)
            inst = found_cls(self._queue, job, job_logger, **job.module_kwargs)
            try:
                job.result = inst.run()
            finally:
                inst.teardown()
        except BaseException:
            job.error = traceback.format_exc()
            self._logger.error("Failed job {}".format(job_id))
            self._logger.error(traceback.format_exc())
            job.state = JobStates.STATE_FAILED
        else:
            job.state = JobStates.STATE_SUCCESS
        finally:
            job.ended = time.time()
            job.log = job_log_buffer.getvalue()
            job_log_buffer.close()

    def run(self, run_once=False):
        self._queue.kz_ses.add_listener(self.__state_handler)
        self._register_worker()

        while True:
            try:
                job_id = self._queue._kz_queue.get()
                self._queue._kz_queue.consume()
                self._logger.info("Handling job {}".format(job_id))
                self._handle_job(job_id)
                self._logger.info("Finished job {}".format(job_id))
            except SystemExit:
                break
            except BaseException:
                self._logger.error(traceback.format_exc())
            finally:
                if run_once:
                    sys.exit(0)


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
    if args.module_path:
        for path in set(args.module_path):
            sys.path.insert(0, path)

    q = Queue(args.zkservers, args.queue)
    w = WorkListener(q)
    w.run(args.run_once)


if __name__ == "__main__":
    main()
