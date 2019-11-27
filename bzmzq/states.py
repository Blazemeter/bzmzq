class _BaseState(object):
    @classmethod
    def __new__(cls, *args, **kwargs):
        return {state_name: state_id
                for state_name, state_id in cls.__dict__.items()
                if state_name.startswith("STATE")}


class JobStates(_BaseState):
    STATE_PENDING = 0
    STATE_RUNNING = 1
    STATE_SUCCESS = 2
    STATE_FAILED = 3


class ScheduledJobStates(_BaseState):
    STATE_DISABLED = 0
    STATE_ENABLED = 1


class WorkerStates(_BaseState):
    STATE_IDLE = 0
    STATE_BUSY = 1
