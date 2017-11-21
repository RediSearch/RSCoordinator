from rmtest import ModuleTestCase
import redis
import unittest
import platform
from base_case import BaseSearchTestCase


class TagsTestCase(BaseSearchTestCase):
    
    def testTagIndex(self):

        self.cmd('ft.broadcast', 'flushdb')

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag'))
        N = 10
        for n in range(N):

            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                   'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n))
        res = self.search('idx', 'hello world')
        self.assertEqual(10, res[0])

        res = self.search('idx', 'foo bar')
        self.assertEqual(0, res[0])

        res = self.search('idx', '@tags:{foo bar}')
        self.assertEqual(N, res[0])

        # inorder should not affect tags
        res = self.search(
            'idx', '@tags:{tag 1} @tags:{foo bar}', 'slop 0', 'inorder')
        self.assertEqual(1, res[0])

        for n in range(N - 1):
            res = self.search(
                'idx', '@tags:{tag %d}' % n, 'nocontent')
            self.assertEqual(1, res[0])
            self.assertEqual('doc%d' % n, res[1])
            res = self.search(
                'idx', '@tags:{tag\\ %d}' % n, 'nocontent')
            self.assertEqual(1, res[0])

            res = self.search(
                'idx', 'hello world @tags:{tag\\ %d|tag %d}' % (n, n + 1), 'nocontent')
            self.assertEqual(2, res[0])
            self.assertEqual('doc%d' % n, res[2])
            self.assertEqual('doc%d' % (n + 1), res[1])

            res = self.search(
                'idx', 'term%d @tags:{tag %d}' % (n, n), 'nocontent')
            self.assertEqual(1, res[0])
            self.assertEqual('doc%d' % n, res[1])

    def testSeparator(self):

        self.cmd('ft.broadcast', 'flushdb')

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ':'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                               'title', 'hello world', 'tags', 'hello world: fooz bar:foo,bar:BOO FAR'))

        for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}'):
            res = self.search('idx', q)
            self.assertEqual(
                1, res[0], msg='Error trying {}'.format(q))

        self.flushdb()

    def testInvalidSyntax(self):
        
        # invalid syntax
        with self.assertResponseError():
            self.cmd(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator')
        with self.assertResponseError():
            self.cmd(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "foo")
        with self.assertResponseError():
            self.cmd(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "")

    def testTagVals(self):

        self.cmd('ft.broadcast', 'flushdb')

        self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'othertags', 'tag')

        N = 100
        alltags = set()
        for n in range(N):
            tags = ('foo %d' % n, 'bar %d' % n)
            alltags.add(tags[0])
            alltags.add(tags[1])
            self.assertCmdOk('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                             'tags', ','.join(tags), 'othertags', 'baz %d' % int(n // 2))

        res = self.cmd('ft.tagvals', 'idx', 'tags')
        self.assertEqual(N * 2, len(res))
        self.assertSetEqual(alltags, set(res))

        res = self.cmd('ft.tagvals', 'idx', 'othertags')
        self.assertEqual(N / 2, len(res))


if __name__ == '__main__':

    unittest.main()
