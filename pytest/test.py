from base_case import BaseSearchTestCase
import redis
import unittest
from hotels import hotels
import random
import itertools


class SearchTestCase(BaseSearchTestCase):

    def testAdd(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                               'title', 'hello world',
                                        'body', 'lorem ist ipsum'))

        ret = itertools.chain(*self.broadcast('keys', '*'))
        for s in ret:
            self.assertTrue(s.startswith('idx:idx{') or s.startswith(
                'doc1{') or s.startswith('ft:idx{'))

    def testUnion(self):

        N = 100
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'f', 'text'))
        for i in range(N):

            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'f', 'hello world' if i % 2 == 0 else 'hallo werld'))

        res = self.cmd(
            'ft.search', 'idx', 'hello|hallo', 'nocontent', 'limit', '0', '100')
        self.assertEqual(N + 1, len(res))
        self.assertEqual(N, res[0])

        res = self.cmd(
            'ft.search', 'idx', 'hello|world', 'nocontent', 'limit', '0', '100')
        self.assertEqual(51, len(res))
        self.assertEqual(50, res[0])

        res = self.cmd('ft.search', 'idx', '(hello|hello)(world|world)',
                       'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(51, len(res))
        self.assertEqual(50, res[0])

        res = self.cmd(
            'ft.search', 'idx', '(hello|hallo)(werld|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(101, len(res))
        self.assertEqual(100, res[0])

        res = self.cmd(
            'ft.search', 'idx', '(hallo|hello)(world|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(101, len(res))
        self.assertEqual(100, res[0])

        res = self.cmd(
            'ft.search', 'idx', '(hello|werld)(hallo|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(101, len(res))
        self.assertEqual(100, res[0])

        res = self.cmd(
            'ft.search', 'idx', '(hello|hallo) world', 'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(51, len(res))
        self.assertEqual(50, res[0])

        res = self.cmd(
            'ft.search', 'idx', '(hello|world)(hallo|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
        self.assertEqual(1, len(res))
        self.assertEqual(0, res[0])

    def testSearch(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 0.5, 'fields',
                               'title', 'hello world',
                                        'body', 'lorem ist ipsum'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello another world',
                                        'body', 'lorem ist ipsum lorem lorem'))

        res = self.cmd('ft.search', 'idx', 'hello')

        self.assertTrue(len(res) == 5)
        self.assertEqual(res[0], 2L)
        self.assertEqual(res[1], "doc2")
        self.assertTrue(isinstance(res[2], list))
        self.assertTrue('title' in res[2])
        self.assertTrue('hello another world' in res[2])
        self.assertEqual(res[3], "doc1")
        self.assertTrue('hello world' in res[4])

        # Test searching with no content
        res = self.cmd(
            'ft.search', 'idx', 'hello', 'nocontent')
        self.assertTrue(len(res) == 3)
        self.assertEqual(res[0], 2L)
        self.assertEqual(res[1], "doc2")
        self.assertEqual(res[2], "doc1")

        # Test searching WITHSCORES
        res = self.cmd(
            'ft.search', 'idx', 'hello', 'WITHSCORES')
        self.assertEqual(len(res), 7)
        self.assertEqual(res[0], 2L)
        self.assertEqual(res[1], "doc2")
        self.assertTrue(float(res[2]) > 0)
        self.assertEqual(res[4], "doc1")
        self.assertTrue(float(res[5]) > 0)

        # Test searching WITHSCORES NOCONTENT
        res = self.cmd(
            'ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
        self.assertEqual(len(res), 5)
        self.assertEqual(res[0], 2L)
        self.assertEqual(res[1], "doc2")
        self.assertTrue(float(res[2]) > 0)
        self.assertEqual(res[3], "doc1")
        self.assertTrue(float(res[4]) > 0)

    def testDelete(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'f', 'text'))

        for i in range(100):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'f', 'hello world'))

        for i in range(100):
            self.assertEqual(1, self.cmd(
                'ft.del', 'idx', 'doc%d' % i))
            # second delete should return 0
            self.assertEqual(0, self.cmd(
                'ft.del', 'idx', 'doc%d' % i))

            res = self.cmd(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
            self.assertNotIn('doc%d' % i, res)
            self.assertEqual(res[0], 100 - i - 1)
            self.assertEqual(len(res), 100 - i)

            # test reinsertion
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'f', 'hello world'))
            res = self.cmd(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
            self.assertIn('doc%d' % i, res)
            self.assertEqual(1, self.cmd(
                'ft.del', 'idx', 'doc%d' % i))
        did = 'rrrr'
        self.assertOk(self.cmd('ft.add', 'idx', did, 1, 'fields',
                               'f', 'hello world'))
        self.assertEqual(1, self.cmd('ft.del', 'idx', did))
        self.assertEqual(0, self.cmd('ft.del', 'idx', did))
        self.assertOk(self.cmd('ft.add', 'idx', did, 1, 'fields',
                               'f', 'hello world'))
        self.assertEqual(1, self.cmd('ft.del', 'idx', did))
        self.assertEqual(0, self.cmd('ft.del', 'idx', did))

    def testReplace(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'f', 'text'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                               'f', 'hello world'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'f', 'hello world'))
        res = self.cmd(
            'ft.search', 'idx', 'hello world')
        self.assertEqual(2, res[0])

        with self.assertResponseError():
            # make sure we can't insert a doc twice
            res = self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                           'f', 'hello world')

        # now replace doc1 with a different content
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'replace', 'fields',
                               'f', 'goodbye universe'))

        # make sure the query for hello world does not return the replaced
        # document
        res = self.cmd(
            'ft.search', 'idx', 'hello world', 'nocontent')
        self.assertEqual(1, res[0])
        self.assertEqual('doc2', res[1])

        # search for the doc's new content
        res = self.cmd(
            'ft.search', 'idx', 'goodbye universe', 'nocontent')
        self.assertEqual(1, res[0])
        self.assertEqual('doc1', res[1])

    def testDrop(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'f', 'text'))

        for i in range(200):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'f', 'hello world'))

        keys = list(itertools.chain(*self.broadcast('keys', '*')))
        self.assertGreaterEqual(len(keys), 200)

        self.assertOk(self.cmd('ft.drop', 'idx'))
        keys = list(itertools.chain(*self.broadcast('keys', '*')))
        self.assertEqual(0, len(keys))

    def testCustomStopwords(self):

        # Index with default stopwords
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'foo', 'text'))

        # Index with custom stopwords
        self.assertOk(self.cmd('ft.create', 'idx2', 'stopwords', 2, 'hello', 'world',
                               'schema', 'foo', 'text'))
        # Index with NO stopwords
        self.assertOk(self.cmd('ft.create', 'idx3', 'stopwords', 0,
                               'schema', 'foo', 'text'))

        for idx in ('idx', 'idx2', 'idx3'):
            self.assertOk(self.cmd('ft.add', idx, 'doc1', 1.0,
                                   'fields', 'foo', 'hello world'))
            self.assertOk(self.cmd('ft.add', idx, 'doc2', 1.0,
                                   'fields', 'foo', 'to be or not to be'))

        # Normal index should return results just for 'hello world'
        self.assertEqual([1, 'doc1'],  self.cmd(
            'ft.search', 'idx', 'hello world', 'nocontent'))
        self.assertEqual([0],  self.cmd(
            'ft.search', 'idx', 'to be or not', 'nocontent'))

        # Custom SW index should return results just for 'to be or not'
        self.assertEqual([0],  self.cmd(
            'ft.search', 'idx2', 'hello world', 'nocontent'))
        self.assertEqual([1, 'doc2'],  self.cmd(
            'ft.search', 'idx2', 'to be or not', 'nocontent'))

        # No SW index should return results for both
        self.assertEqual([1, 'doc1'],  self.cmd(
            'ft.search', 'idx3', 'hello world', 'nocontent'))
        self.assertEqual([1, 'doc2'],  self.cmd(
            'ft.search', 'idx3', 'to be or not', 'nocontent'))

    def testOptional(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0,
                               'fields', 'foo', 'hello wat woot'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0,
                               'fields', 'foo', 'hello world woot'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc3', 1.0,
                               'fields', 'foo', 'hello world werld'))

        res = self.cmd('ft.search', 'idx', 'hello', 'nocontent', )
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
        res = self.cmd('ft.search', 'idx', 'hello world',
                       'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([2L, 'doc3', 'doc2'], res)
        res = self.cmd('ft.search', 'idx', 'hello ~world',
                       'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
        res = self.cmd('ft.search', 'idx', 'hello ~world ~werld',
                       'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)

    def testExplain(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
        q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
        res = self.cmd('ft.explain', 'idx', q)
        # print res

        self.assertEqual("""INTERSECT {
  UNION {
    hello
    +hello(expanded)
  }
  UNION {
    world
    +world(expanded)
  }
  EXACT {
    what
    what
  }
  UNION {
    UNION {
      hello
      +hello(expanded)
    }
    UNION {
      world
      +world(expanded)
    }
  }
  UNION {
    NUMERIC {10.000000 <= @bar <= 100.000000}
    NUMERIC {200.000000 <= @bar <= 300.000000}
  }
}
""", res)

    #@unittest.expectedFailure

    def testPaging(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
        N = 100
        for i in range(N):
            self.assertOk(self.cmd('ft.add', 'idx', '%d' % i, float(i + 1) / float(N), 'fields',
                                   'foo', 'hello', 'bar', i))

        chunk = 7
        offset = 0
        while True:

            res = self.cmd('ft.search', 'idx', 'hello',
                           'nocontent', 'limit', offset, chunk)
            self.assertEqual(res[0], N)

            if offset + chunk > N:
                self.assertTrue(len(res) - 1 <= chunk)
                break
            self.assertEqual(len(res), chunk + 1)
            for n, id in enumerate(res[1:]):
                self.assertEqual(int(id), N - 1 - (offset + n))
            offset += chunk
            chunk = random.randrange(1, 10)
        res = self.cmd('ft.search', 'idx', 'hello', 'nocontent',
                       'sortby', 'bar', 'asc', 'limit', N, 10)
        self.assertEqual(res[0], N)
        self.assertEqual(len(res), 1)

    def testPrefix(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        N = 100
        for i in range(N):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'foo', 'constant term%d' % (random.randrange(0, 5))))
        res = self.cmd('ft.search', 'idx', 'constant term', 'nocontent')
        self.assertEqual([0], res)
        res = self.cmd('ft.search', 'idx', 'constant term*', 'nocontent')
        self.assertEqual(N, res[0])
        res = self.cmd('ft.search', 'idx', 'const* term*', 'nocontent')
        self.assertEqual(N, res[0])
        res = self.cmd('ft.search', 'idx', 'constant term1*', 'nocontent')
        self.assertGreater(res[0], 2)
        res = self.cmd('ft.search', 'idx', 'const* -term*', 'nocontent')
        self.assertEqual([0], res)
        res = self.cmd('ft.search', 'idx', 'constant term9*', 'nocontent')
        self.assertEqual([0], res)

    def testNoIndex(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema',
            'foo', 'text',
            'num', 'numeric', 'sortable', 'noindex',
            'extra', 'text', 'noindex', 'sortable'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', '0.1', 'fields',
                               'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
        res = self.cmd('ft.search', 'idx', 'hello world', 'nocontent')
        self.assertListEqual([1, 'doc1'], res)
        res = self.cmd('ft.search', 'idx', 'lorem ipsum', 'nocontent')
        self.assertListEqual([0], res)
        res = self.cmd('ft.search', 'idx', '@num:[1 1]', 'nocontent')
        self.assertListEqual([0], res)

    def testPartial(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema',
            'foo', 'text',
            'num', 'numeric', 'sortable', 'noindex',
            'extra', 'text', 'noindex'))
        # print self.cmd('ft.info', 'idx')

        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', '0.1', 'fields',
                               'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', '0.1', 'fields',
                               'foo', 'hello world', 'num', 2, 'extra', 'abba'))
        res = self.cmd('ft.search', 'idx', 'hello world', 'sortby',
                       'num', 'asc', 'nocontent', 'withsortkeys')
        self.assertListEqual([2L, 'doc1', '#1', 'doc2', '#2'], res)
        res = self.cmd('ft.search', 'idx', 'hello world', 'sortby',
                       'num', 'desc', 'nocontent', 'withsortkeys')
        self.assertListEqual([2L, 'doc2', '#2', 'doc1', '#1'], res)

        # Updating non indexed fields doesn't affect search results
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                               'fields', 'num', 3, 'extra', 'jorem gipsum'))
        res = self.cmd('ft.search', 'idx', 'hello world',
                       'sortby', 'num', 'desc',)
        self.assertListEqual([2L, 'doc1', ['foo', 'hello world', 'num', '3', 'extra', 'jorem gipsum'],
                              'doc2', ['foo', 'hello world', 'num', '2', 'extra', 'abba']], res)
        res = self.cmd('ft.search', 'idx', 'hello', 'nocontent', 'withscores')
        # Updating only indexed field affects search results
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                               'fields', 'foo', 'wat wet'))
        res = self.cmd('ft.search', 'idx', 'hello world', 'nocontent')
        self.assertListEqual([1L, 'doc2'], res)
        res = self.cmd('ft.search', 'idx', 'wat', 'nocontent')
        self.assertListEqual([1L, 'doc1'], res)

        # Test updating of score and no fields
        res = self.cmd('ft.search', 'idx', 'wat', 'nocontent', 'withscores')
        self.assertLess(float(res[2]), 1)
        #self.assertListEqual([1L, 'doc1'], res)
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1',
                               '1.0', 'replace', 'partial', 'fields'))
        res = self.cmd('ft.search', 'idx', 'wat', 'nocontent', 'withscores')
        self.assertGreater(float(res[2]), 1)

        # Test updating payloads
        res = self.cmd('ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
        self.assertIsNone(res[2])
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', '1.0',
                               'replace', 'partial', 'payload', 'foobar', 'fields'))
        res = self.cmd('ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
        self.assertEqual('foobar', res[2])

    def testSortBy(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable'))
        N = 100
        for i in range(N):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'foo', 'hello%03d world' % i, 'bar', 100 - i))

        res = self.cmd('ft.search', 'idx', 'world',
                       'nocontent', 'sortby', 'foo')
        self.assertListEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                              'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = self.cmd('ft.search', 'idx', 'world',
                       'nocontent', 'sortby', 'foo', 'desc')
        self.assertListEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                              'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = self.cmd('ft.search', 'idx', 'world',
                       'nocontent', 'sortby', 'bar', 'desc')
        self.assertListEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                              'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
        res = self.cmd('ft.search', 'idx', 'world',
                       'nocontent', 'sortby', 'bar', 'asc')
        self.assertListEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                              'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
        res = self.cmd('ft.search', 'idx', 'world', 'nocontent',
                       'sortby', 'bar', 'desc', 'withsortkeys', 'limit', '2', '5')
        self.assertListEqual(
            [100L, 'doc2', '#98', 'doc3', '#97', 'doc4', '#96', 'doc5', '#95', 'doc6', '#94'], res)

    def testNestedIntersection(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text'))
        for i in range(20):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, float(i + 1) / 20, 'fields',
                                   'a', 'foo', 'b', 'bar', 'c', 'baz', 'd', 'gaz'))
        res = [
            self.cmd('ft.search', 'idx', 'foo bar baz gaz', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@a:foo @b:bar @c:baz @d:gaz', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@b:bar @a:foo @c:baz @d:gaz', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@c:baz @b:bar @a:foo @d:gaz', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@d:gaz @c:baz @b:bar @a:foo', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@a:foo (@b:bar (@c:baz @d:gaz))', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@c:baz (@a:foo (@b:bar (@c:baz @d:gaz)))', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@b:bar (@a:foo (@c:baz @d:gaz))', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     '@d:gaz (@a:foo (@c:baz @b:bar))', 'nocontent'),
            self.cmd('ft.search', 'idx', 'foo (bar baz gaz)', 'nocontent'),
            self.cmd('ft.search', 'idx', 'foo (bar (baz gaz))', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     'foo (bar (foo bar) (foo bar))', 'nocontent'),
            self.cmd('ft.search', 'idx',
                     'foo (foo (bar baz (gaz)))', 'nocontent'),
            self.cmd('ft.search', 'idx', 'foo (foo (bar (baz (gaz (foo bar (gaz))))))', 'nocontent')]

        for r in res:
            self.assertListEqual(res[0], r)

    def testNot(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        N = 100
        for i in range(N):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'foo', 'constant term%d' % (random.randrange(0, 5))))

        for i in range(5):
            inclusive = self.cmd(
                'ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)

            exclusive = self.cmd(
                'ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
            # print inclusive, exclusive
            self.assertNotEqual(inclusive[0], N)
            self.assertEqual(inclusive[0] + exclusive[0], N)

            s1, s2 = set(inclusive[1:]), set(exclusive[1:])
            self.assertTrue(s1.difference(s2) == s1)
            self.assertTrue(s2.intersection(s1) == set())

        # NOT on a non existing term
        self.assertEqual(self.cmd('ft.search', 'idx',
                                  'constant -dasdfasdf', 'nocontent')[0], N)
        # not on self term
        self.assertEqual(self.cmd('ft.search', 'idx',
                                  'constant -constant', 'nocontent'), [0])

        self.assertEqual(self.cmd(
            'ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent'), [0])
        #self.assertEqual(self.cmd('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

    def testInKeys(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text'))

        for i in range(200):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'foo', 'hello world'))

        for keys in (
            ['doc%d' % i for i in range(10)], ['doc%d' % i for i in range(0, 30, 2)], [
                'doc%d' % i for i in range(99, 0, -5)]
        ):
            res = self.cmd(
                'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', len(keys), *keys)
            self.assertEqual(len(keys), res[0])
            self.assertTrue(all((k in res for k in keys)))

        self.assertEqual(0, self.cmd(
            'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', 4, 'foo', 'bar', 'baz')[0])

    def testSlopInOrder(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1, 'fields',
                               'title', 't1 t2'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1, 'fields',
                               'title', 't1 t3 t2'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc3', 1, 'fields',
                               'title', 't1 t3 t4 t2'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc4', 1, 'fields',
                               'title', 't1 t3 t4 t5 t2'))

        res = self.cmd(
            'ft.search', 'idx', 't1|t4 t3|t2', 'slop', '0', 'inorder', 'nocontent')
        self.assertEqual({'doc3', 'doc4', 'doc2', 'doc1'}, set(res[1:]))
        res = self.cmd(
            'ft.search', 'idx', 't2 t1', 'slop', '0', 'nocontent')
        self.assertEqual(1, res[0])
        self.assertEqual('doc1', res[1])
        self.assertEqual(0, self.cmd(
            'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder')[0])
        self.assertEqual(1, self.cmd(
            'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder')[0])

        self.assertEqual(2, self.cmd(
            'ft.search', 'idx', 't1 t2', 'slop', '1', 'inorder')[0])
        self.assertEqual(3, self.cmd(
            'ft.search', 'idx', 't1 t2', 'slop', '2', 'inorder')[0])
        self.assertEqual(4, self.cmd(
            'ft.search', 'idx', 't1 t2', 'slop', '3', 'inorder')[0])
        self.assertEqual(4, self.cmd(
            'ft.search', 'idx', 't1 t2', 'inorder')[0])
        self.assertEqual(0, self.cmd(
            'ft.search', 'idx', 't t1', 'inorder')[0])
        self.assertEqual(2, self.cmd(
            'ft.search', 'idx', 't1 t2 t3 t4')[0])
        self.assertEqual(0, self.cmd(
            'ft.search', 'idx', 't1 t2 t3 t4', 'inorder')[0])

    def testExact(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 0.5, 'fields',
                               'title', 'hello world',
                                        'body', 'lorem ist ipsum'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello another world',
                                        'body', 'lorem ist ipsum lorem lorem'))

        res = self.cmd(
            'ft.search', 'idx', '"hello world"', 'verbatim')
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])

        res = self.cmd(
            'ft.search', 'idx', "hello \"another world\"", 'verbatim')
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

    def testGeo(self):

        gsearch = lambda query, lon, lat, dist, unit='km', *args: self.cmd(
            'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit, *args)

        self.assertOk(self.cmd('ft.create', 'idx',
                               'schema', 'name', 'text', 'location', 'geo'))

        for i, hotel in enumerate(hotels):
            self.assertOk(self.cmd('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                                   hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])))
        res = self.cmd('ft.search', 'idx', 'hilton')
        self.assertEqual(len(hotels), res[0])

        res = gsearch('hilton', "-0.1757", "51.5156", '1')
        self.assertEqual(3, res[0])
        self.assertEqual('hotel2', res[5])
        self.assertEqual('hotel21', res[3])
        self.assertEqual('hotel79', res[1])

        res = gsearch('hilton', "-0.1757", "51.5156", '10',
                      'km', 'nocontent', 'limit', 0, 20)
        self.assertEqual(14, res[0])
        self.assertEqual('hotel93', res[1])
        self.assertEqual('hotel92', res[2])
        self.assertEqual('hotel79', res[3])

        res2 = gsearch('hilton', "-0.1757", "51.5156", '10000',
                       'm', 'nocontent', 'limit', 0, 20)
        self.assertEqual(set(res), set(res2))

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
        self.assertEqual(1, res[0])
        self.assertEqual('hotel94', res[1])

        res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
        self.assertEqual(5, res[0])
        self.assertIn('hotel94', res)

        res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
        self.assertEqual(3, res[0])
        self.assertIn('hotel94', res)

    @unittest.expectedFailure
    def testAddHash(self):

        self.assertOk(self.cmd('ft.create', 'idx', 'schema',
                               'title', 'text', 'weight', 10.0, 'body', 'text', 'price', 'numeric'))

        self.assertTrue(
            self.redis().hmset('doc1', {"title": "hello world", "body": "lorem ipsum", "price": 2}))
        self.assertTrue(
            self.redis().hmset('doc2', {"title": "hello werld", "body": "lorem ipsum", "price": 5}))

        self.assertOk(self.cmd('ft.addhash', 'idx', 'doc1', 1.0))
        self.assertOk(self.cmd('ft.addhash', 'idx', 'doc2', 1.0))

        res = self.cmd('ft.search', 'idx', "hello", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc1", res[2])
        self.assertEqual("doc2", res[1])

        res = self.cmd(
            'ft.search', 'idx', "hello", "filter", "price", "0", "3")
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])
        self.assertListEqual(
            ['body', 'lorem ipsum', 'price', '2', 'title', 'hello world'], res[2])

        res = self.cmd(
            'ft.search', 'idx', "hello werld", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

    def testInfields(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text', 'weight', 1.0))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 0.5, 'fields',
                               'title', 'hello world',
                                        'body', 'lorem ipsum'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello world lorem ipsum',
                                        'body', 'hello world'))

        res = self.cmd(
            'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "title", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc2", res[1])
        self.assertEqual("doc1", res[2])

        res = self.cmd(
            'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = self.cmd(
            'ft.search', 'idx', 'hello', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = self.cmd(
            'ft.search', 'idx',  '\"hello world\"', 'verbatim', "infields", 1, "body", "nocontent")

        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = self.cmd(
            'ft.search', 'idx', '\"lorem ipsum\"', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])

        res = self.cmd(
            'ft.search', 'idx', 'lorem ipsum', "infields", 2, "body", "title", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc2", res[1])
        self.assertEqual("doc1", res[2])

    def testScorerSelection(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))

        # this is the default scorer
        res = self.cmd('ft.search', 'idx', 'foo', 'scorer', 'TFIDF')
        self.assertEqual(res, [0])
        with self.assertResponseError():
            res = self.cmd('ft.search', 'idx', 'foo', 'scorer', 'NOSUCHSCORER')

    def testFieldSelectors(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'TiTle', 'text', 'BoDy', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1, 'fields',
                               'title', 'hello world', 'body', 'foo bar'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 0.5, 'fields',
                               'body', 'hello world', 'title', 'foo bar'))

        res = self.cmd('ft.search', 'idx', '@title:hello world', 'nocontent')
        self.assertEqual(res, [1, 'doc1'])
        res = self.cmd('ft.search', 'idx', '@body:hello world', 'nocontent')
        self.assertEqual(res, [1, 'doc2'])

        res = self.cmd('ft.search', 'idx',
                       '@body:hello @title:world', 'nocontent')
        self.assertEqual(res, [0])

        res = self.cmd('ft.search', 'idx',
                       '@body:hello world @title:world', 'nocontent')
        self.assertEqual(res, [0])
        res = self.cmd('ft.search', 'idx',
                       '@BoDy:(hello|foo) @Title:(world|bar)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = self.cmd('ft.search', 'idx',
                       '@body:(hello|foo world|bar)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = self.cmd('ft.search', 'idx',
                       '@body|title:(hello world)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

    def testStemming(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 0.5, 'fields',
                               'title', 'hello kitty'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello kitties'))

        res = self.cmd(
            'ft.search', 'idx', 'hello kitty', "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])

        res = self.cmd(
            'ft.search', 'idx', 'hello kitty', "nocontent", "verbatim")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

    def testExpander(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 0.5, 'fields',
                               'title', 'hello kitty'))

        res = self.cmd(
            'ft.search', 'idx', 'kitties', "nocontent", "expander", "SBSTEM")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

        res = self.cmd(
            'ft.search', 'idx', 'kitties', "nocontent", "expander", "noexpander")
        self.assertEqual(1, len(res))
        self.assertEqual(0, res[0])

        res = self.cmd(
            'ft.search', 'idx', 'kitti', "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

        res = self.cmd(
            'ft.search', 'idx', 'kitti', "nocontent", 'verbatim')
        self.assertEqual(1, len(res))
        self.assertEqual(0, res[0])

        # Calling a stem directly works even with VERBATIM.
        # You need to use the + prefix escaped
        res = self.cmd(
            'ft.search', 'idx', '\\+kitti', "nocontent", 'verbatim')
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

    def testNumericRange(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric'))
        for i in xrange(100):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                   'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i))

        res = self.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                       "filter", "score", 0, 100)

        self.assertEqual(11, len(res))
        self.assertEqual(100, res[0])

        res = self.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                       "filter", "score", 0, 50)
        self.assertEqual(51, res[0])
        res = self.cmd('ft.search', 'idx', 'hello kitty', 'verbatim', "nocontent", "limit", 0, 100,
                       "filter", "score", "(0", "(50")

        self.assertEqual(49, res[0])
        res = self.cmd('ft.search', 'idx', 'hello kitty', "nocontent",
                       "filter", "score", "-inf", "+inf")
        self.assertEqual(100, res[0])

        # test multi filters
        scrange = (19, 90)
        prrange = (290, 385)
        res = self.cmd('ft.search', 'idx', 'hello kitty',
                       "filter", "score", scrange[
                                    0], scrange[1],
                       "filter", "price", prrange[0], prrange[1])

        # print res
        for doc in res[2::2]:

            sc = int(doc[doc.index('score') + 1])
            pr = int(doc[doc.index('price') + 1])

            self.assertTrue(sc >= scrange[0] and sc <= scrange[1])
            self.assertGreaterEqual(pr, prrange[0])
            self.assertLessEqual(pr, prrange[1])

        self.assertEqual(10, res[0])

        res = self.cmd('ft.search', 'idx', 'hello kitty',
                       "filter", "score", "19", "90",
                       "filter", "price", "90", "185")

        self.assertEqual(0, res[0])

        # Test numeric ranges as part of query syntax
        res = self.cmd('ft.search', 'idx',
                       'hello kitty @score:[0 100]', "nocontent")

        self.assertEqual(11, len(res))
        self.assertEqual(100, res[0])

        res = self.cmd('ft.search', 'idx',
                       'hello kitty  @score:[0 50]', "nocontent")
        self.assertEqual(51, res[0])
        res = self.cmd('ft.search', 'idx',
                       'hello kitty @score:[(0 (50]', 'verbatim', "nocontent")
        self.assertEqual(49, res[0])
        res = self.cmd('ft.search', 'idx',
                       '@score:[(0 (50]', 'verbatim', "nocontent")
        self.assertEqual(49, res[0])
        res = self.cmd('ft.search', 'idx',
                       'hello kitty -@score:[(0 (50]', 'verbatim', "nocontent")
        self.assertEqual(51, res[0])
        res = self.cmd('ft.search', 'idx',
                       'hello kitty @score:[-inf +inf]', "nocontent")
        self.assertEqual(100, res[0])

    def testSuggestions(self):

        self.assertEqual(1, self.cmd(
            'ft.SUGADD', 'ac', 'hello world', 1))
        self.assertEqual(1, self.cmd(
            'ft.SUGADD', 'ac', 'hello world', 1, 'INCR'))

        res = self.cmd("FT.SUGGET", "ac", "hello")
        self.assertEqual(1, len(res))
        self.assertEqual("hello world", res[0])

        terms = ["hello werld", "hallo world",
                 "yellow world", "wazzup", "herp", "derp"]
        sz = 2
        for term in terms:
            self.assertEqual(sz, self.cmd(
                'ft.SUGADD', 'ac', term, sz - 1))
            sz += 1

        self.assertEqual(7, self.cmd('ft.SUGLEN', 'ac'))

        # search not fuzzy
        self.assertEqual(["hello world", "hello werld"],
                         self.cmd("ft.SUGGET", "ac", "hello"))

        # print  self.cmd("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1", "WITHSCORES")
        # search fuzzy - shuold yield more results
        self.assertEqual(['hello world', 'hello werld', 'yellow world', 'hallo world'],
                         self.cmd("ft.SUGGET", "ac", "hello", "FUZZY"))

        # search fuzzy with limit of 1
        self.assertEqual(['hello world'],
                         self.cmd("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1"))

        # scores should return on WITHSCORES
        rc = self.cmd(
            "ft.SUGGET", "ac", "hello", "WITHSCORES")
        self.assertEqual(4, len(rc))
        self.assertTrue(float(rc[1]) > 0)
        self.assertTrue(float(rc[3]) > 0)

        rc = self.cmd("ft.SUGDEL", "ac", "hello world")
        self.assertEqual(1L, rc)
        rc = self.cmd("ft.SUGDEL", "ac", "world")
        self.assertEqual(0L, rc)

        rc = self.cmd("ft.SUGGET", "ac", "hello")
        self.assertEqual(['hello werld'], rc)

    def testPayload(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'f', 'text'))
        for i in range(10):

            self.assertOk(self.cmd('ft.add', 'idx', '%d' % i, 1.0,
                                   'payload', 'payload %d' % i,
                                   'fields', 'f', 'hello world'))
        res = self.cmd(
            'ft.search', 'idx', 'hello world')
        self.assertEqual(21, len(res))

        res = self.cmd(
            'ft.search', 'idx', 'hello world', 'withpayloads')

        self.assertEqual(31, len(res))
        self.assertEqual(10, res[0])
        for i in range(1, 30, 3):
            self.assertEqual(res[i + 1], 'payload %s' % res[i])

    def testGet(self):

        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'text'))

        for i in range(100):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                   'foo', 'hello world', 'bar', 'wat wat'))

        for i in range(100):
            res = self.cmd('ft.get', 'idx', 'doc%d' % i)
            self.assertIsNotNone(res)
            self.assertListEqual(['foo', 'hello world', 'bar', 'wat wat'], res)

        rr = self.cmd('ft.mget', 'idx', *('doc%d' % i for i in range(100)))
        self.assertEqual(len(rr), 100)
        for res in rr:
            self.assertIsNotNone(res)
            self.assertListEqual(['foo', 'hello world', 'bar', 'wat wat'], res)

    def testInfoCommand(self):
        from itertools import combinations

        self.assertCmdOk('ft.create', 'idx', 'NOFIELDS',
                         'schema', 'title', 'text')
        for i in xrange(50):
            self.assertCmdOk('ft.add', 'idx', 'doc%d' %
                             i, 1, 'fields', 'title', 'hello term%d' % i)

        bcast_info = self.broadcast('_ft.info', 'idx')
        combined = {}
        SUM_FIELDS = ('num_docs', 'num_terms', 'num_records',
                      'inverted_size_mb', 'doc_table_size_mb', 'offset_vectors_sz_mb',
                      'key_table_size_mb')
        AVG_FIELDS = ('records_per_doc_avg', 'bytes_per_doc_avg', 'offsets_per_term_avg',
                      'bytes_per_record_avg', 'offset_bits_per_record_avg',)
        MAX_FIELDS = ('max_doc_id',)

        GC_FIELDS = {
            'current_hz': 'avg',
            'bytes_collected': 'total',
            'effectiv_cycles_rate': 'avg'
        }

        def mkdict(arr_reply):
            return dict(zip(*[iter(arr_reply)] * 2))

        for info in bcast_info:
            dd = mkdict(info)
            # Sums
            for k, v in dd.items():
                if k in ('index_name', 'index_options', 'fields'):
                    continue
                if k in SUM_FIELDS:
                    combined.setdefault(k, 0.0)
                    combined[k] += float(v)
                elif k in AVG_FIELDS:
                    combined.setdefault(k, [])
                    combined[k].append(float(v))
                elif k in MAX_FIELDS:
                    if k in combined:
                        combined[k] = max(combined[k], float(v))
                    else:
                        combined[k] = float(v)

            gc = combined.setdefault('gc_stats', {})
            ddgc = mkdict(dd['gc_stats'])
            for fld, typ in GC_FIELDS.items():
                if typ == 'avg':
                    gc.setdefault(fld, []).append(float(ddgc[fld]))
                else:
                    gc.setdefault(fld, 0.0)
                    gc[fld] += float(ddgc[fld])

        def mkavgs(dst, fldnames):
            for fldname in fldnames:
                if fldname in dst:
                    dst[fldname] = sum(dst[fldname]) / len(dst[fldname])

        mkavgs(combined, AVG_FIELDS)
        mkavgs(combined['gc_stats'], [
               k for k, v in GC_FIELDS.items() if v == 'avg'])

        res = self.cmd('ft.info', 'idx')
        d = mkdict(res)

        self.assertEqual(d['index_name'], 'idx')
        self.assertEqual(d['index_options'], ['NOFIELDS'])
        self.assertListEqual(
            d['fields'], [['title', 'type', 'TEXT', 'WEIGHT', '1']])

        self.assertEquals(int(d['num_docs']), combined['num_docs'])
        self.assertEquals(int(d['num_terms']), combined['num_terms'])
        self.assertEquals(int(d['max_doc_id']), combined['max_doc_id'])
        self.assertEquals(int(d['records_per_doc_avg']), 2)
        self.assertEquals(int(d['num_records']), combined['num_records'])

        self.assertGreater(float(d['offset_vectors_sz_mb']), 0)
        self.assertGreater(float(d['key_table_size_mb']), 0)
        self.assertGreater(float(d['inverted_sz_mb']), 0)
        self.assertGreater(float(d['bytes_per_record_avg']), 0)
        self.assertGreater(float(d['doc_table_size_mb']), 0)

        d['gc_stats'] = mkdict(d['gc_stats'])
        for k in d['gc_stats']:
            d['gc_stats'][k] = float(d['gc_stats'][k])

        self.assertEqual(d['gc_stats'], combined['gc_stats'])

        for x in range(1, 5):
            for combo in combinations(('NOOFFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
                combo = list(filter(None, combo))
                options = combo + ['schema', 'f1', 'text']
                try:
                    self.cmd('ft.drop', 'idx')
                except:
                    pass
                self.assertCmdOk('ft.create', 'idx', *options)
                info = self.cmd('ft.info', 'idx')
                ix = info.index('index_options')
                self.assertFalse(ix == -1)

                opts = info[ix + 1]
                # make sure that an empty opts string returns no options in
                # info
                if not combo:
                    self.assertListEqual([], opts)

                for option in filter(None, combo):
                    self.assertTrue(option in opts)

    def testReturning(self):
        self.cmd('FT.BROADCAST', 'FLUSHDB')

        self.assertCmdOk('ft.create', 'idx', 'schema', 'f1',
                         'text', 'f2', 'text', 'n1', 'numeric', 'f3', 'text')
        for i in range(10):
            self.assertCmdOk('ft.add', 'idx', 'DOC_{0}'.format(i), 1.0, 'fields',
                             'f2', 'val2', 'f1', 'val1', 'f3', 'val3',
                             'n1', i)

        # RETURN 0. Simplest case
        for x in self.retry_with_rdb_reload():
            res = self.cmd('ft.search', 'idx', 'val*', 'return', '0')
            self.assertEqual(11, len(res))
            self.assertEqual(10, res[0])
            for r in res[1:]:
                self.assertTrue(r.startswith('DOC_'))

        for field in ('f1', 'f2', 'f3', 'n1'):
            res = self.cmd('ft.search', 'idx', 'val*', 'return', 1, field)
            self.assertEqual(21, len(res))
            self.assertEqual(10, res[0])
            for pair in grouper(res[1:], 2):
                docname, fields = pair
                self.assertEqual(2, len(fields))
                self.assertEqual(field, fields[0])
                self.assertTrue(docname.startswith('DOC_'))

        # Test when field is not found
        res = self.cmd('ft.search', 'idx', 'val*', 'return', 1, 'nonexist')
        self.assertEqual(21, len(res))
        self.assertEqual(10, res[0])
        for pair in grouper(res[1:], 2):
            _, pair = pair
            self.assertEqual(None, pair[1])

    def testNoStem(self):
        self.cmd('FT.BROADCAST', 'FLUSHDB')

        self.cmd('ft.create', 'idx', 'schema', 'body',
                 'text', 'name', 'text', 'nostem')
        for _ in self.retry_with_rdb_reload():
            try:
                self.cmd('ft.del', 'idx', 'doc')
            except redis.ResponseError:
                pass

            # Insert a document
            self.assertCmdOk('ft.add', 'idx', 'doc', 1.0, 'fields',
                             'body', "located",
                             'name', "located")

            # Now search for the fields
            res_body = self.cmd('ft.search', 'idx', '@body:location')
            res_name = self.cmd('ft.search', 'idx', '@name:location')
            self.assertEqual(0, res_name[0])
            self.assertEqual(1, res_body[0])

    def testSearchNonexistField(self):
        self.cmd('FT.BROADCAST', 'FLUSHDB')
        # GH Issue 133
        self.cmd('ft.create', 'idx', 'schema', 'title', 'text',
                 'weight', 5.0, 'body', 'text', 'url', 'text')
        self.cmd('ft.add', 'idx', 'd1', 1.0, 'nosave', 'fields', 'title',
                 'hello world', 'body', 'lorem dipsum', 'place', '-77.0366 38.8977')
        self.cmd('ft.search', 'idx', 'Foo', 'GEOFILTER',
                 'place', '-77.0366', '38.8977', '1', 'km')

    def testSortbyMissingField(self):
        # GH Issue 131
        self.cmd('FT.BROADCAST', 'FLUSHDB')
        self.cmd('ft.create', 'ix', 'schema', 'txt',
                 'text', 'num', 'numeric', 'sortable')
        self.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo')
        self.cmd('ft.search', 'ix', 'foo', 'sortby', 'num')

    def testParallelIndexing(self):
        # GH Issue 207
        self.cmd('FT.BROADCAST', 'FLUSHDB')
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        from threading import Thread
        self.client()
        ndocs = 100

        def runner(tid):
            cli = self.client()
            for num in range(ndocs):
                cli.execute_command('ft.add', 'idx', 'doc{}_{}'.format(tid, num), 1.0,
                                    'fields', 'txt', 'hello world' * 20)
        ths = []
        for tid in range(10):
            ths.append(Thread(target=runner, args=(tid,)))

        [th.start() for th in ths]
        [th.join() for th in ths]
        res = self.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        self.assertEqual(1000, int(d['num_docs']))


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


if __name__ == '__main__':

    unittest.main()
