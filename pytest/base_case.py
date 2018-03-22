from rmtest.cluster import ClusterModuleTestCase
import redis
import unittest
from contextlib import contextmanager
import time


class BaseSearchTestCase(ClusterModuleTestCase('../src/module-oss.so',
                                               num_nodes=3, module_args=['PARTITIONS', 'AUTO'])):

    def setUp(self):
        # Update all the nodes
        self.client().execute_command('ft.clusterrefresh')
        time.sleep(0.5)
        self.broadcast('ft.clusterrefresh')

        self.flushdb()

    def search(self, *args):
        return self.client().execute_command('ft.search', *args)

    def broadcast(self, cmd, *args):
        return self.client().execute_command('ft.broadcast', cmd, *args)

    def flushdb(self):
        self.broadcast('flushdb')

    @contextmanager
    def redis(self):

        yield self.client()
