class cached_prop(object):
    def __init__(self, fn):
        self.__doc__ = getattr(fn, '__doc__')
        self.fn = fn

    def __get__(self, obj, cls):
        if obj is None:
            return self

        value = self.fn(obj)
        if value:
            obj.__dict__[self.fn.__name__] = value
        return value


class ZkPath(list):
    SEP = '/'

    @property
    def path(self):
        return self.SEP + self.SEP.join([str(x) for x in list(self)])

    def __add__(self, other):
        result = super(ZkPath, self).__add__(other)
        return ZkPath(result)

    def __str__(self):
        return self.path


def get_logger(name):
    import sys
    import logging
    import os

    logger = logging.getLogger(name)
    lh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    lh.setFormatter(formatter)
    logger.addHandler(lh)

    log_level = os.environ.get('BZMZQ_LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)

    if 'NO_BZMZQ_LOGS' in os.environ:
        logger.disabled = True

    return logger
