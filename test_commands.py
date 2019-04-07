import unittest

from time import sleep
from unittest import TestCase
from redistimeseries.client import Client as RedisTimeSeries
from redis import ResponseError

rts = None
port = 6379

class RedisTimeSeriesTest(TestCase):
    def setUp(self):
        global rts
        rts = RedisTimeSeries(port=port)
        rts.flushdb()

    def testCreate(self):
        '''Test TS.CREATE calls'''

        self.assertTrue(rts.tsCreate(1))
        self.assertTrue(rts.tsCreate(2, retentionSecs=5))
        self.assertTrue(rts.tsCreate(3, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsCreate(4, retentionSecs=20, labels={'Time':'Series'}))
        info = rts.tsInfo(4)
        self.assertEqual(20, info['retentionSecs'])
        self.assertEqual(b'Time', info['labels'][0][0])
        
    def testAdd(self):
        '''Test TS.ADD calls'''

        self.assertTrue(rts.tsAdd(1, 1, 1))
        self.assertTrue(rts.tsAdd(2, 2, 3, retentionSecs=10))
        self.assertTrue(rts.tsAdd(3, 3, 2, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsAdd(5, 4, 2, retentionSecs=10, labels={'Redis':'Labs'}))
        self.assertTrue(rts.tsAdd(4, '*', 1))
        info = rts.tsInfo(5)
        self.assertEqual(10, info['retentionSecs'])
        self.assertEqual(b'Redis', info['labels'][0][0])

    def testIncrbyDecrby(self):
        '''Test TS.INCRBY and TS.DECRBY calls'''

        #test without counter reset
        for _ in range(100):
            self.assertTrue(rts.tsIncreaseBy(1,1))
        self.assertEqual(100, rts.tsGet(1)[1])
        for _ in range(100):
            self.assertTrue(rts.tsDecreaseBy(1,1))
        self.assertEqual(0, rts.tsGet(1)[1])

        #test with counter reset
        for _ in range(50):
            self.assertTrue(rts.tsIncreaseBy(1,1,timeBucket=1))  
            self.assertTrue(rts.tsDecreaseBy(2,1,timeBucket=1)) 
        sleep(1.1)
        self.assertTrue(rts.tsIncreaseBy(1,1,timeBucket=1))  
        self.assertEqual(1, rts.tsGet(1)[1])
        self.assertTrue(rts.tsDecreaseBy(2,1,timeBucket=1))  
        self.assertEqual(-1, rts.tsGet(2)[1])

    def testCreateRule(self):
        '''Test TS.CREATERULE and TS.DELETERULE calls'''

        # test rule creation
        rts.tsCreate(1)
        rts.tsCreate(2)
        rts.tsCreateRule(1, 2, 'avg', 1)
        for _ in range(50):
            rts.tsAdd(1, '*', 1)
            rts.tsAdd(1, '*', 2)
        self.assertEqual(rts.tsGet(2)[1], 1.5)
        info = rts.tsInfo(1)
        self.assertEqual(info['rules'][0][1], 1)

        # test rule deletion
        rts.tsDeleteRule(1, 2)
        info = rts.tsInfo(1)
        self.assertFalse(info['rules'])

    def testRange(self):
        '''Test TS.RANGE calls which returns range by key'''
        for i in range(100):
            rts.tsAdd(1, i, i % 7)
        self.assertTrue(100, len(rts.tsRange(1, 0, 200)))
        for i in range(100):
            rts.tsAdd(1, i+200, i % 7)
        self.assertTrue(200, len(rts.tsRange(1, 0, 500)))
        self.assertTrue(20, len(rts.tsRange(1, 0, 500, aggregationType='avg', bucketSizeSeconds=10)))

    def testMultiRange(self):
        '''Test TS.MRANGE calls which returns range by filter'''

        for i in range(100):
            rts.tsAdd(1, i, i % 7, labels={'Test':'This'})
        self.assertTrue(100, len(rts.tsMultiRange(0, 200, filters=['Test=This'])))
        for i in range(100):
            rts.tsAdd(1, i+200, i % 7)
        self.assertTrue(20, len(rts.tsMultiRange(0, 500, filters=['Test=This'],
                        aggregationType='avg', bucketSizeSeconds=10)))

    def testGet(self):
        rts.tsAdd(1, 2, 3)
        self.assertEqual(2, rts.tsGet(1)[0])
        rts.tsAdd(1, 3, 4)
        self.assertEqual(4, rts.tsGet(1)[1])    

    def testInfo(self):
        rts.tsCreate(1, retentionSecs=5, labels={'label' : 'data'})
        info = rts.tsInfo(1)
        self.assertTrue(info['retentionSecs'] == 5)
        self.assertTrue(info['labels'][0][0].decode() == 'label')
        self.assertTrue(info['labels'][0][1].decode() == 'data')


if __name__ == '__main__':
    unittest.main()