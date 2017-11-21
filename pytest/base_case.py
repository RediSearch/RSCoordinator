from rmtest.cluster import ClusterModuleTestCase
import redis
import unittest
from contextlib import contextmanager
class BaseSearchTestCase(ClusterModuleTestCase('../src/module.so')):

    def setUp(self):
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

    

