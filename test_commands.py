import unittest
from unittest import TestCase
from redistimeseries.client import Client as RedisTimeSeries
from redis import ResponseError

rj = None
port = 6379

class RedisTimeSeriesTest(TestCase):
    def setUp(self):
        global rts
        rts = RedisTimeSeries(port=port)
        rts.flushdb()

    def testCreate(self):
        'Test different TS.CREATE calls'
        self.assertTrue(rts.tsCreate(1))
        self.assertTrue(rts.tsCreate(2, retention=5))
        self.assertTrue(rts.tsCreate(3, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsCreate(4, retention=20, labels={'Time':'Series'}))
        info = rts.tsInfo(4)
        self.assertEqual(20, info[3])
        self.assertEqual(b'Time', info[9][0][0])
        
    def testAdd(self):
        self.assertTrue(rts.tsAdd(1, 1, 1))
        self.assertTrue(rts.tsAdd(2, 2, 3, retention=10))
        self.assertTrue(rts.tsAdd(3, 3, 2, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsAdd(5, 4, 2, retention=10, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsAdd(4, '*', 1))
        info = rts.tsInfo(5)
        self.assertEqual(10, info[3])
        self.assertEqual(b'Redis', info[9][0][0])

    def testRange(self):
        '''Test range by key'''
        for i in range(100):
            rts.tsAdd(1, i, i % 7)
        self.assertTrue(100, len(rts.tsRange(1, 0, 200)))
        for i in range(100):
            rts.tsAdd(1, i+200, i % 7)
        self.assertTrue(200, len(rts.tsRange(1, 0, 500)))
        self.assertTrue(20, len(rts.tsRange(1, 0, 500, aggregationType='avg', bucketSizeSeconds=10)))

    def testMultiRange(self):
        '''Test range by label'''
        for i in range(100):
            rts.tsAdd(1, i, i % 7, labels={'Test':'This'})
        self.assertTrue(100, len(rts.tsMultiRange(0, 200, filters=['Test=This'])))
        for i in range(100):
            rts.tsAdd(1, i+200, i % 7)
        self.assertTrue(20, len(rts.tsMultiRange(0, 500, filters=['Test=This'],
                        aggregationType='avg', bucketSizeSeconds=10)))

    def testGet(self):
        rts.tsAdd(1, 2, 3)
        self.assertEqual([2, b'3'], rts.tsGet(1))
        rts.tsAdd(1, 3, 4)
        self.assertEqual([3, b'4'], rts.tsGet(1))
    

if __name__ == '__main__':
    unittest.main()