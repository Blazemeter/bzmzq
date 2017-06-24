from abc import ABCMeta, abstractmethod


class IJobWorker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def setup(self, queue, **kwargs): pass

    @abstractmethod
    def run(self): pass

    @abstractmethod
    def teardown(self): pass
