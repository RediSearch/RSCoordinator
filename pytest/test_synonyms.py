from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time
from base_case import BaseSearchTestCase


class SynonymsTestCase(BaseSearchTestCase):

    def testBasicSynonymsUseCase(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child'), 0)

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a boy',
                                         'body', 'this is a test'))

        res = self.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

    def testTermOnTwoSynonymsGroup(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child'), 0)
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'offspring'), 1)

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a boy',
                                         'body', 'this is a test'))

        res = self.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])
        res = self.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

    def testSynonymGroupWithThreeSynonyms(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a boy',
                                         'body', 'this is a test'))

        res = self.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])
        res = self.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

    def testSynonymWithMultipleDocs(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a boy',
                                         'body', 'this is a test'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                         'title', 'she is a girl',
                                         'body', 'the child sister'))

        res = self.cmd('ft.search', 'idx', 'offspring', 'EXPANDER', 'SYNONYM')
        self.assertEqual(res, [2L, 'doc2', ['title', 'she is a girl', 'body', 'the child sister'], 'doc1', ['title', 'he is a boy', 'body', 'this is a test']])

    def testSynonymUpdate(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a baby',
                                         'body', 'this is a test'))

        self.assertOk(self.cmd('ft.synupdate', 'idx', '0', 'baby'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                         'title', 'he is another baby',
                                         'body', 'another test'))

        res = self.cmd('ft.search', 'idx', 'child', 'EXPANDER', 'SYNONYM')
        # synonyms are applied from the moment they were added, previuse docs are not reindexed
        self.assertEqual(res, [1L, 'doc2', ['title', 'he is another baby', 'body', 'another test']])

    def testSynonymDump(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'baby', 'child'), 1)
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'tree', 'wood'), 2)
        self.assertEqual(self.cmd('ft.syndump', 'idx'), ['baby', [1L], 'offspring', [0L], 'wood', [2L], 'tree', [2L], 'child', [0L, 1L], 'boy', [0L]])

    def testSynonymAddWorngArity(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        exceptionStr = None
        try:
            self.cmd('ft.synadd', 'idx')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'wrong number of arguments for \'ft.synadd\' command')

    def testSynonymAddUnknownIndex(self):
        self.cmd('ft.broadcast', 'flushdb')
        exceptionStr = None
        try:
            self.cmd('ft.synadd', 'idx', 'boy', 'child')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'Unknown index name')

    def testSynonymUpdateWorngArity(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.cmd('ft.synadd', 'idx', 'boy', 'child')
        exceptionStr = None
        try:
            self.cmd('ft.synupdate', 'idx', '0')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual('wrong number of arguments' in exceptionStr, True)

    def testSynonymUpdateUnknownIndex(self):
        self.cmd('ft.broadcast', 'flushdb')
        exceptionStr = None
        try:
            self.cmd('ft.synupdate', 'idx', '0', 'child')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'Unknown index name')

    def testSynonymUpdateNotNumberId(self):
        self.cmd('ft.broadcast', 'flushdb')
        exceptionStr = None
        try:
            self.cmd('ft.synupdate', 'idx', 'test', 'child')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'wrong parameters, id is not an integer')

    def testSynonymUpdateOutOfRangeId(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.cmd('ft.synadd', 'idx', 'boy', 'child')
        exceptionStr = None
        try:
            self.cmd('ft.synupdate', 'idx', '1', 'child')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'given id does not exists')

    def testSynonymDumpWorngArity(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.cmd('ft.synadd', 'idx', 'boy', 'child')
        exceptionStr = None
        try:
            self.cmd('ft.syndump')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'wrong number of arguments for \'ft.syndump\' command')

    def testSynonymUnknownIndex(self):
        self.cmd('ft.broadcast', 'flushdb')
        exceptionStr = None
        try:
            self.cmd('ft.syndump', 'idx')
        except Exception as e:
            exceptionStr = str(e)
        self.assertEqual(exceptionStr, 'Unknown index name')

    def testTwoSynonymsSearch(self):
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy', 'child', 'offspring'), 0)
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                         'title', 'he is a boy child boy',
                                         'body', 'another test'))

        res = self.cmd('ft.search', 'idx', 'offspring offspring', 'EXPANDER', 'SYNONYM')
        # synonyms are applied from the moment they were added, previuse docs are not reindexed
        self.assertEqual(res, [1L, 'doc1', ['title', 'he is a boy child boy', 'body', 'another test']])

    def testSynonymsIntensiveLoad(self):
        iterations = 1000
        self.cmd('ft.broadcast', 'flushdb')
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        for i in range(iterations):
            self.assertEqual(self.cmd('ft.synadd', 'idx', 'boy%d' % i, 'child%d' % i, 'offspring%d' % i), i)
        for i in range(iterations):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                             'title', 'he is a boy%d' % i,
                                             'body', 'this is a test'))
        for i in range(iterations):
            res = self.cmd('ft.search', 'idx', 'child%d' % i, 'EXPANDER', 'SYNONYM')
            self.assertEqual(res, [1L, 'doc%d' % i, ['title', 'he is a boy%d' % i, 'body', 'this is a test']])
