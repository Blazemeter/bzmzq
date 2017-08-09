from abc import ABCMeta, abstractmethod


class IJobWorker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, queue, job, logger, **kwargs):
        self.queue = queue
        self.job = job
        self.logger = logger

    @abstractmethod
    def run(self): pass

    @abstractmethod
    def teardown(self): pass
