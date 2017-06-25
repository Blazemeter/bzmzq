import argparse
import time
from threading import Event, Lock, Timer

from kazoo.exceptions import NoNodeError

from helpers import get_logger
from job import Job
from queue import Queue
from scheduled_job import ScheduledJob
from states import JobStates, ScheduledJobStates


class Scheduler(object):
    ZK_SCHEDULER_LOCK_NAME = 'scheduler-lock'
    JOB_CLEANER_INTERVAL = 1

    def __init__(self, queue):
        self._queue = queue
        self._logger = get_logger(self.__class__.__name__)
        self._timers_lock = Lock()
        self._timers = {}  # {<scheduled_job_id> : Timer}
        self._more_events = Event()

        self._timer_update_thread = None

    def __enabled_scheduled_job_update_handler(self, _):
        if self._timer_update_thread and self._timer_update_thread.is_alive():
            self._more_events.set()

        self._timer_update_thread = Timer(
            0, self._thread_scan_and_update_timers)
        self._timer_update_thread.daemon = True
        self._timer_update_thread.start()

    def _register_handlers(self):
        enabled_scheduled_jobs_path = str(
            self._queue.path_factory.scheduled_job_state.id(
                ScheduledJobStates.STATE_ENABLED))
        self._queue._kz_ses.ChildrenWatch(
            enabled_scheduled_jobs_path,
            self.__enabled_scheduled_job_update_handler)

    def _cancel_all_timers(self):
        with self._timers_lock:
            for _, timer in self._timers.iteritems():
                timer.cancel()
            self._timers = {}

    def _set_scheduled_job_timer(self, scheduled_job):
        with self._timers_lock:
            current_timer = self._timers.get(scheduled_job.id)
            if current_timer:
                current_timer.cancel()

            delta = 0 if not scheduled_job.next_run \
                else scheduled_job.next_run - time.time()

            new_timer = Timer(
                delta, self._thread_push_job, args=(
                    scheduled_job.id,))
            self._timers[scheduled_job] = new_timer
            new_timer.start()

    def _thread_scan_and_update_timers(self):
        self._more_events.clear()
        try:
            self._cancel_all_timers()
            scheduled_jobs = self._queue.get_scheduled_jobs(
                state=ScheduledJobStates.STATE_ENABLED)
            for scheduled_job in scheduled_jobs:
                try:
                    self._set_scheduled_job_timer(scheduled_job)
                except NoNodeError:
                    pass
        finally:
            if self._more_events.is_set():
                self._thread_scan_and_update_timers()

    def _can_push_job(self, scheduled_job):
        if not scheduled_job.concurrent and scheduled_job.last_job_id:
            try:
                last_job = Job(self._queue, scheduled_job.last_job_id)
                _, last_job_state_id = last_job.state
                if last_job_state_id == JobStates.STATE_RUNNING:
                    return False
            except NoNodeError:
                pass
        return True

    def _thread_push_job(self, scheduled_job_id):
        try:
            scheduled_job = ScheduledJob(self._queue, scheduled_job_id)
            self._logger.info(
                "Pushing job scheduled job {}".format(
                    scheduled_job.id))

            if self._can_push_job(scheduled_job):
                job = Job.create(
                    self._queue,
                    scheduled_job.name,
                    scheduled_job.module,
                    scheduled_job.module_kwargs,
                    scheduled_job.priority)
                scheduled_job.last_job_id = job.id

            new_next_run = time.time() + scheduled_job.interval_min * 60
            scheduled_job.next_run = new_next_run
            self._set_scheduled_job_timer(scheduled_job)
        except NoNodeError:
            pass

    def _thread_clean_stale_job_loop(self):
        try:
            self._logger.info("Job cleaner is running...")
            for job in self._queue.get_jobs(state=JobStates.STATE_RUNNING):
                worker_path = self._queue.path_factory.worker.id(job.worker)
                print self._queue._kz_ses.exists(str(worker_path))
                if not self._queue._kz_ses.exists(str(worker_path)):
                    self._logger.warn("Cleaning stale job [{}]".format(job.id))
                    job.state = JobStates.STATE_FAILED
        except NoNodeError:
            pass
        finally:
            self._logger.info("Job cleaner finished!")
            t = Timer(self.JOB_CLEANER_INTERVAL*60, self._thread_clean_stale_job_loop)
            t.daemon = True
            t.start()

    def run(self):
        self._logger.info("Scheduler acquiring lock...")
        try:
            with self._queue.get_lock(self.ZK_SCHEDULER_LOCK_NAME):
                self._logger.info("Scheduler is running!")
                self._thread_clean_stale_job_loop()
                self._register_handlers()
                while True:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            self._cancel_all_timers()
            exit(1)


def main():
    parser = argparse.ArgumentParser(description='BzmZQ Scheduler')
    parser.add_argument(
        '-z', '--zkservers',
        type=str,
        required=True,
        help='Zookeeper servers. "127.0.0.1:2181,127.0.0.1:2182"')
    parser.add_argument(
        '-q',
        '--queue',
        type=str,
        required=True,
        help='Queue name')

    args = parser.parse_args()
    q = Queue(args.zkservers, args.queue)
    s = Scheduler(q)
    s.run()

if __name__ == "__main__":
    main()