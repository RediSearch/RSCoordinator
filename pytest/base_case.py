from rmtest import ModuleTestCase
import redis
import unittest

class BaseSearchTestCase(ModuleTestCase('../src/module.so')):

    @classmethod
    def broadcast(cls, r, cmd, *args):
        return r.execute_command('ft.broadcast', cmd, *args)

    def search(self, r, *args):
        return r.execute_command('ft.search', *args)
