from abc import ABCMeta, abstractmethod


class IJobWorker(object, metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, queue, job, logger, **kwargs):
        self.queue = queue
        self.job = job
        self.logger = logger

    @abstractmethod
    def run(self): pass

    @abstractmethod
    def teardown(self): pass
