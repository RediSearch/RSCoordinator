import rmtest.config
from oss_tests.base_case import FTBaseCaseMethods


class BaseSearchTestCase(rmtest.ClusterTestCase, FTBaseCaseMethods):
    @classmethod
    def get_module_args(cls):
        print("Getting module args!!!")
        return super(BaseSearchTestCase, cls).get_module_args() + ['PARTITIONS', 'AUTO']

    def setUp(self):
        # Update all the nodes
        r = self.client
        r.execute_command('ft.clusterrefresh')
        # time.sleep(0.5)
        self.broadcast('ft.clusterrefresh')

    def broadcast(self, cmd, *args):
        return self.client.execute_command('ft.broadcast', cmd, *args)

    def flushdb(self):
        self.broadcast('flushdb')
