# -*- coding: utf-8 -*-

from base_case import BaseSearchTestCase
import redis
import unittest
import os

SRCTEXT = './cn_sample.txt'
GENTXT = './genesis.txt'


class CnTestCase(BaseSearchTestCase):

    def testCn(self):
        text = open(SRCTEXT).read()
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'LANGUAGE',
                 'CHINESE', 'FIELDS', 'txt', text)
        res = self.cmd('ft.search', 'idx', '之旅', 'SUMMARIZE',
                       'HIGHLIGHT', 'LANGUAGE', 'chinese')
        self.assertEqual([1L, 'doc1', ['txt', '2009\xe5\xb9\xb4\xef\xbc\x98\xe6\x9c\x88\xef\xbc\x96\xe6\x97\xa5\xe5\xbc\x80\xe5\xa7\x8b\xe5\xa4\xa7\xe5\xad\xa6<b>\xe4\xb9\x8b\xe6\x97\x85</b>\xef\xbc\x8c\xe5\xb2\xb3\xe9\x98\xb3\xe4\xbb\x8a\xe5\xa4\xa9\xe7\x9a\x84\xe6\xb0\x94\xe6\xb8\xa9\xe4\xb8\xba38.6\xe2\x84\x83, \xe4\xb9\x9f\xe5\xb0\xb1\xe6\x98\xaf101.48\xe2\x84\x89... \xef\xbc\x8c \xe5\x8d\x95\xe4\xbd\x8d \xe5\x92\x8c \xe5\x85\xa8\xe8\xa7\x92 : 2009\xe5\xb9\xb4 8\xe6\x9c\x88 6\xe6\x97\xa5 \xe5\xbc\x80\xe5\xa7\x8b \xe5\xa4\xa7\xe5\xad\xa6 <b>\xe4\xb9\x8b\xe6\x97\x85</b> \xef\xbc\x8c \xe5\xb2\xb3\xe9\x98\xb3 \xe4\xbb\x8a\xe5\xa4\xa9 \xe7\x9a\x84 \xe6\xb0\x94\xe6\xb8\xa9 \xe4\xb8\xba 38.6\xe2\x84\x83 , \xe4\xb9\x9f\xe5\xb0\xb1\xe6\x98\xaf 101... ']], res)

        res = self.cmd('ft.search', 'idx', 'hacker', 'summarize', 'highlight')
        self.assertEqual([1L, 'doc1', ['txt', ' visit http://code.google.com/p/jcseg, we all admire the <b>hacker</b> spirit!\xe7\x89\xb9\xe6\xae\x8a\xe6\x95\xb0\xe5\xad\x97: \xe2\x91\xa0 \xe2\x91\xa9 \xe2\x91\xbd \xe3\x88\xa9. ... p / jcseg , we all admire appreciate like love enjoy the <b>hacker</b> spirit mind ! \xe7\x89\xb9\xe6\xae\x8a \xe6\x95\xb0\xe5\xad\x97 : \xe2\x91\xa0 \xe2\x91\xa9 \xe2\x91\xbd \xe3\x88\xa9 . ~~~ ... ']], res)

        # Check that we can tokenize english with friso (sub-optimal, but don't
        # want gibberish)
        gentxt = open(GENTXT).read()
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'LANGUAGE',
                 'chinese', 'FIELDS', 'txt', gentxt)
        res = self.cmd('ft.search', 'idx', 'abraham', 'summarize', 'highlight')
        self.assertEqual([1L, 'doc2', [
                         'txt', 'thy name any more be called Abram, but thy name shall be <b>Abraham</b>; for a father of many nations have I made thee. {17:6} And... and I will be their God. {17:9} And God said unto <b>Abraham</b>, Thou shalt keep my covenant therefore, thou, and thy seed... hath broken my covenant. {17:15} And God said unto <b>Abraham</b>, As for Sarai thy wife, thou shalt not call her name Sarai... ']], res)

        # Add an empty document. Hope we don't crash!
        self.cmd('ft.add', 'idx', 'doc3', 1.0, 'language',
                 'chinese', 'fields', 'txt1', '')

        # Check splitting. TODO - see how to actually test for matches
        self.cmd('ft.search', 'idx', 'redis客户端', 'language', 'chinese')
        self.cmd('ft.search', 'idx', '简介Redisson 是一个高级的分布式协调Redis客户端',
                 'language', 'chinese')

if __name__ == '__main__':

    unittest.main()
