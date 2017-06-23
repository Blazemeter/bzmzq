from threading import Lock as TLock, _get_ident

from kazoo.recipe.lock import Lock


class RLock(Lock):
    def __init__(self, client, path, identifier=None):
        super(RLock, self).__init__(client, path, identifier)
        self._reference_lock = TLock()
        self._lock_holding_thread = None
        self._reference_count = 0

    def acquire(self, blocking=True, timeout=None):
        with self._reference_lock:
            # You are the man, bump the ref count
            if self._lock_holding_thread == _get_ident():
                self._reference_count += 1
                return True

        lock_result = super(RLock, self).acquire(blocking, timeout)

        if lock_result:
            with self._reference_lock:
                self._lock_holding_thread = _get_ident()
                self._reference_count = 1
        return lock_result

    def __rlock_cleanup(self):
        self._lock_holding_thread = None
        self._reference_count = 0

    def release(self):
        with self._reference_lock:
            # You never had the lock :(
            if self._lock_holding_thread != _get_ident():
                return False

            self._reference_count -= 1
            if self._reference_count == 0:
                self.__rlock_cleanup()
                return self.client.retry(self._inner_release)
            return True
