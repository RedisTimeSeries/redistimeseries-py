import unittest
import time
from time import sleep
from unittest import TestCase
from redistimeseries.client import Client as RedisTimeSeries

rts = None
port = 6379

class RedisTimeSeriesTest(TestCase):
    def setUp(self):
        global rts
        rts = RedisTimeSeries(port=port)
        rts.flushdb()

    def testCreate(self):
        '''Test TS.CREATE calls'''

        self.assertTrue(rts.create(1))
        self.assertTrue(rts.create(2, retention_msecs=5))
        self.assertTrue(rts.create(3, labels={'Redis':'Labs'}))
        self.assertTrue(rts.create(4, retention_msecs=20, labels={'Time':'Series'}))
        info = rts.info(4)
        self.assertEqual(20, info.retention_msecs)
        self.assertEqual('Series', info.labels['Time'])

    def testAlter(self):
        '''Test TS.ALTER calls'''

        rts.create(1)
        self.assertEqual(0, rts.info(1).retention_msecs)
        rts.alter(1, retention_msecs=10)
        self.assertEqual({}, rts.info(1).labels)
        self.assertEqual(10, rts.info(1).retention_msecs)
        rts.alter(1, labels={'Time':'Series'})
        self.assertEqual('Series', rts.info(1).labels['Time'])
        self.assertEqual(10, rts.info(1).retention_msecs)

    def testAdd(self):
        '''Test TS.ADD calls'''

        self.assertEqual(1, rts.add(1, 1, 1))
        self.assertEqual(2, rts.add(2, 2, 3, retention_msecs=10))
        self.assertEqual(3, rts.add(3, 3, 2, labels={'Redis':'Labs'}))
        self.assertEqual(4, rts.add(4, 4, 2, retention_msecs=10, labels={'Redis':'Labs', 'Time':'Series'}))
        
        self.assertEqual(int(time.time() * 1000), rts.add(5, '*', 1))
        info = rts.info(4)
        self.assertEqual(10, info.retention_msecs)
        self.assertEqual('Labs', info.labels['Redis'])

    def testMAdd(self):
        '''Test TS.MADD calls'''

        rts.create('a')
        self.assertEqual([1, 2, 3], rts.madd([('a', 1, 5), ('a', 2, 10), ('a', 3, 15)]))
        res = rts.madd([('a', '*', 5), ('a', '*', 10), ('a', '*', 15)])
        self.assertTrue(res[0] + 10 >= res[2])

    def testIncrbyDecrby(self):
        '''Test TS.INCRBY and TS.DECRBY calls'''

        #test without counter reset
        for _ in range(100):
            self.assertTrue(rts.incrby(1,1))
        self.assertEqual(100, rts.get(1)[1])
        for _ in range(100):
            self.assertTrue(rts.decrby(1,1))
        self.assertEqual(0, rts.get(1)[1])

        #test with counter reset
        for _ in range(50):
            self.assertTrue(rts.incrby(1,1,time_bucket=100))  
            self.assertTrue(rts.decrby(2,1,time_bucket=100)) 
        sleep(0.2)
        self.assertTrue(rts.incrby(1,1,time_bucket=1))  
        self.assertEqual(1, rts.get(1)[1])
        self.assertTrue(rts.decrby(2,1,time_bucket=1))  
        self.assertEqual(-1, rts.get(2)[1])

    def testCreateRule(self):
        '''Test TS.CREATERULE and TS.DELETERULE calls'''

        # test rule creation
        rts.create(1)
        rts.create(2)
        rts.createrule(1, 2, 'avg', 1000)
        for _ in range(50):
            rts.add(1, '*', 1)
            rts.add(1, '*', 2)
        self.assertAlmostEqual(rts.get(2)[1], 1.5)
        info = rts.info(1)
        self.assertEqual(info.rules[0][1], 1000)

        # test rule deletion
        rts.deleterule(1, 2)
        info = rts.info(1)
        self.assertFalse(info.rules)

    def testRange(self):
        '''Test TS.RANGE calls which returns range by key'''

        for i in range(100):
            rts.add(1, i, i % 7)
        self.assertTrue(100, len(rts.range(1, 0, 200)))
        for i in range(100):
            rts.add(1, i+200, i % 7)
        self.assertTrue(200, len(rts.range(1, 0, 500)))
        self.assertTrue(20, len(rts.range(1, 0, 500, aggregation_type='avg', bucket_size_msec=10)))

    def testMultiRange(self):
        '''Test TS.MRANGE calls which returns range by filter'''

        rts.create(1, labels={'Test':'This'})
        rts.create(2, labels={'Test':'This', 'Taste':'That'})
        for i in range(100):
            rts.add(1, i, i % 7)
            rts.add(2, i, i % 11)
        self.assertTrue(100, len(rts.mrange(0, 200, filters=['Test=This'])))
        for i in range(100):
            rts.add(1, i+200, i % 7)
        self.assertTrue(20, len(rts.mrange(0, 500, filters=['Test=This'],
                        aggregation_type='avg', bucket_size_msec=10)))

    def testGet(self):
        '''Test TS.GET calls'''

        rts.add(1, 2, 3)
        self.assertEqual(2, rts.get(1)[0])
        rts.add(1, 3, 4)
        self.assertEqual(4, rts.get(1)[1])    

    def testMGet(self):
        '''Test TS.MGET calls'''
        rts.create(1, labels={'Test':'This'})
        rts.create(2, labels={'Test':'This', 'Taste':'That'})
        rts.add(1, '*', 15)
        rts.add(2, '*', 25)
        res = rts.mget(['Test=This'])
        self.assertEqual('15', res[0]['1'][2])
        self.assertEqual('25', res[1]['2'][2])
        res = rts.mget(['Taste=That'])
        self.assertEqual('25', res[0]['2'][2])

    def testInfo(self):
        '''Test TS.INFO calls'''
        rts.create(1, retention_msecs=5, labels={'currentLabel' : 'currentData'})
        info = rts.info(1)
        self.assertTrue(info.retention_msecs == 5)
        self.assertEqual(info.labels['currentLabel'], 'currentData')

    def testQueryIndex(self):
        rts.create(1, labels={'Test':'This'})
        rts.create(2, labels={'Test':'This', 'Taste':'That'})
        self.assertEqual(2, len(rts.queryindex(['Test=This'])))       
        self.assertEqual(1, len(rts.queryindex(['Taste=That'])))       
        self.assertEqual(['2'], rts.queryindex(['Taste=That']))       

if __name__ == '__main__':
    unittest.main()