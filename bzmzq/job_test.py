from ijobworker import IJobWorker


class MyTestModule(IJobWorker):
    def setup(self, queue, **kwargs):
        print "setup"

    def run(self):
        print "run"

    def teardown(self):
        print "twardown"
