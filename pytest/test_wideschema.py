from rmtest import ModuleTestCase
import redis
import unittest
import platform
from base_case import BaseSearchTestCase


class SearchTestCase(BaseSearchTestCase):

    def testWideSchema(self):

        schema = []
        FIELDS = 128 if platform.architecture()[0] == '64bit' else 64
        for i in range(FIELDS):
            schema.extend(('field_%d' % i, 'TEXT'))
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', *schema))
        N = 10
        for n in range(N):
            fields = []
            for i in range(FIELDS):
                fields.extend(('field_%d' % i, 'hello token_%d' % i))
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' %
                                   n, 1.0, 'fields', *fields))
        for _ in self.retry_with_rdb_reload():
            for i in range(FIELDS):

                res = self.search('idx', '@field_%d:token_%d' %
                                  (i, i), 'NOCONTENT')
                self.assertEqual(res[0], N)

                res = self.cmd('ft.explain', 'idx',
                               '@field_%d:token_%d' % (i, i)).strip()
                self.assertEqual(
                    '@field_%d:UNION {\n  @field_%d:token_%d\n  @field_%d:+token_%d(expanded)\n}' % (i, i, i, i, i), res)

                res = self.search(
                    'idx', 'hello @field_%d:token_%d' % (i, i), 'NOCONTENT')
                self.assertEqual(res[0], N)

            res = self.search('idx', ' '.join(
                ('@field_%d:token_%d' % (i, i) for i in range(FIELDS))))
            self.assertEqual(res[0], N)

            res = self.search('idx', ' '.join(('token_%d' % (i)
                                               for i in range(FIELDS))))
            self.assertEqual(res[0], N)

if __name__ == '__main__':

    unittest.main()
