import unittest
import time
from time import sleep
from unittest import TestCase
from redistimeseries.client import Client as RedisTimeSeries
from redis import Redis

version = None
rts = None
port = 6379


class RedisTimeSeriesTest(TestCase):
    def setUp(self):
        global rts
        global version
        rts = RedisTimeSeries(port=port)
        rts.redis.flushdb()
        modules = rts.redis.execute_command("module", "list")
        if modules is not None:
            for module_info in modules:
                if module_info[1] == b'timeseries':
                    version = int(module_info[3])

    def testVersionRuntime(self):
        import redistimeseries as rts_pkg
        self.assertNotEqual("", rts_pkg.__version__)

    def testCreate(self):
        '''Test TS.CREATE calls'''
        self.assertTrue(rts.create(1))
        self.assertTrue(rts.create(2, retention_msecs=5))
        self.assertTrue(rts.create(3, labels={'Redis': 'Labs'}))
        self.assertTrue(rts.create(4, retention_msecs=20, labels={'Time': 'Series'}))
        info = rts.info(4)
        self.assertEqual(20, info.retention_msecs)
        self.assertEqual('Series', info.labels['Time'])

        if version is None or version < 14000:
            return

        # Test for a chunk size of 128 Bytes
        self.assertTrue(rts.create("time-serie-1", chunk_size=128))
        info = rts.info("time-serie-1")
        self.assertEqual(128, info.chunk_size)

        # Test for duplicate policy
        for duplicate_policy in ["block", "last", "first", "min", "max"]:
            ts_name = "time-serie-ooo-{0}".format(duplicate_policy)
            self.assertTrue(rts.create(ts_name, duplicate_policy=duplicate_policy))
            info = rts.info(ts_name)
            self.assertEqual(duplicate_policy, info.duplicate_policy)

    def testAlter(self):
        '''Test TS.ALTER calls'''

        self.assertTrue(rts.create(1))
        self.assertEqual(0, rts.info(1).retention_msecs)
        self.assertTrue(rts.alter(1, retention_msecs=10))
        self.assertEqual({}, rts.info(1).labels)
        self.assertEqual(10, rts.info(1).retention_msecs)
        self.assertTrue(rts.alter(1, labels={'Time': 'Series'}))
        self.assertEqual('Series', rts.info(1).labels['Time'])
        self.assertEqual(10, rts.info(1).retention_msecs)
        pipe = rts.pipeline()
        self.assertTrue(pipe.create(2))

        if version is None or version < 14000:
            return
        info = rts.info(1)
        self.assertEqual(None, info.duplicate_policy)
        self.assertTrue(rts.alter(1, duplicate_policy='min'))
        info = rts.info(1)
        self.assertEqual('min', info.duplicate_policy)

    def testAdd(self):
        '''Test TS.ADD calls'''

        self.assertEqual(1, rts.add(1, 1, 1))
        self.assertEqual(2, rts.add(2, 2, 3, retention_msecs=10))
        self.assertEqual(3, rts.add(3, 3, 2, labels={'Redis': 'Labs'}))
        self.assertEqual(4, rts.add(4, 4, 2, retention_msecs=10, labels={'Redis': 'Labs', 'Time': 'Series'}))
        self.assertAlmostEqual(time.time(), float(rts.add(5, '*', 1)) / 1000, 2)

        info = rts.info(4)
        self.assertEqual(10, info.retention_msecs)
        self.assertEqual('Labs', info.labels['Redis'])

        if version is None or version < 14000:
            return

        # Test for a chunk size of 128 Bytes on TS.ADD
        self.assertTrue(rts.add("time-serie-1", 1, 10.0, chunk_size=128))
        info = rts.info("time-serie-1")
        self.assertEqual(128, info.chunk_size)

        # Test for duplicate policy BLOCK
        self.assertEqual(1, rts.add("time-serie-add-ooo-block", 1, 5.0))
        try:
            rts.add("time-serie-add-ooo-block", 1, 5.0, duplicate_policy='block')
        except Exception as e:
            self.assertEqual("TSDB: Error at upsert, update is not supported in BLOCK mode", e.__str__())

        # Test for duplicate policy LAST
        self.assertEqual(1, rts.add("time-serie-add-ooo-last", 1, 5.0))
        self.assertEqual(1, rts.add("time-serie-add-ooo-last", 1, 10.0, duplicate_policy='last'))
        self.assertEqual(10.0, rts.get("time-serie-add-ooo-last")[1])

        # Test for duplicate policy FIRST
        self.assertEqual(1, rts.add("time-serie-add-ooo-first", 1, 5.0))
        self.assertEqual(1, rts.add("time-serie-add-ooo-first", 1, 10.0, duplicate_policy='first'))
        self.assertEqual(5.0, rts.get("time-serie-add-ooo-first")[1])

        # Test for duplicate policy MAX
        self.assertEqual(1, rts.add("time-serie-add-ooo-max", 1, 5.0))
        self.assertEqual(1, rts.add("time-serie-add-ooo-max", 1, 10.0, duplicate_policy='max'))
        self.assertEqual(10.0, rts.get("time-serie-add-ooo-max")[1])

        # Test for duplicate policy MIN
        self.assertEqual(1, rts.add("time-serie-add-ooo-min", 1, 5.0))
        self.assertEqual(1, rts.add("time-serie-add-ooo-min", 1, 10.0, duplicate_policy='min'))
        self.assertEqual(5.0, rts.get("time-serie-add-ooo-min")[1])

    def testMAdd(self):
        '''Test TS.MADD calls'''

        rts.create('a')
        self.assertEqual([1, 2, 3], rts.madd([('a', 1, 5), ('a', 2, 10), ('a', 3, 15)]))

    def testIncrbyDecrby(self):
        '''Test TS.INCRBY and TS.DECRBY calls'''

        for _ in range(100):
            self.assertTrue(rts.incrby(1, 1))
            sleep(0.001)
        self.assertEqual(100, rts.get(1)[1])
        for _ in range(100):
            self.assertTrue(rts.decrby(1, 1))
            sleep(0.001)
        self.assertEqual(0, rts.get(1)[1])

        self.assertTrue(rts.incrby(2, 1.5, timestamp=5))
        self.assertEqual((5, 1.5), rts.get(2))
        self.assertTrue(rts.incrby(2, 2.25, timestamp=7))
        self.assertEqual((7, 3.75), rts.get(2))
        self.assertTrue(rts.decrby(2, 1.5, timestamp=15))
        self.assertEqual((15, 2.25), rts.get(2))
        if version is None or version < 14000:
            return

        # Test for a chunk size of 128 Bytes on TS.INCRBY
        self.assertTrue(rts.incrby("time-serie-1", 10, chunk_size=128))
        info = rts.info("time-serie-1")
        self.assertEqual(128, info.chunk_size)

        # Test for a chunk size of 128 Bytes on TS.DECRBY
        self.assertTrue(rts.decrby("time-serie-2", 10, chunk_size=128))
        info = rts.info("time-serie-2")
        self.assertEqual(128, info.chunk_size)

    def testDelRange(self):
        '''Test TS.DEL calls'''

        try:
            rts.delrange('test', 0, 100)
        except Exception as e:
            self.assertEqual("TSDB: the key does not exist", e.__str__())

        for i in range(100):
            rts.add(1, i, i % 7)
        self.assertEqual(22, rts.delrange(1, 0, 21))
        self.assertEqual([], rts.range(1, 0, 21))
        self.assertEqual([(22, 1.0)], rts.range(1, 22, 22))

    def testCreateRule(self):
        '''Test TS.CREATERULE and TS.DELETERULE calls'''

        # test rule creation
        time = 100
        rts.create(1)
        rts.create(2)
        rts.createrule(1, 2, 'avg', 100)
        for i in range(50):
            rts.add(1, time + i * 2, 1)
            rts.add(1, time + i * 2 + 1, 2)
        rts.add(1, time * 2, 1.5)
        self.assertAlmostEqual(rts.get(2)[1], 1.5)
        info = rts.info(1)
        self.assertEqual(info.rules[0][1], 100)

        # test rule deletion
        rts.deleterule(1, 2)
        info = rts.info(1)
        self.assertFalse(info.rules)

    def testRange(self):
        '''Test TS.RANGE calls which returns range by key'''

        for i in range(100):
            rts.add(1, i, i % 7)
        self.assertEqual(100, len(rts.range(1, 0, 200)))
        for i in range(100):
            rts.add(1, i + 200, i % 7)
        self.assertEqual(200, len(rts.range(1, 0, 500)))
        # last sample isn't returned
        self.assertEqual(20, len(rts.range(1, 0, 500, aggregation_type='avg', bucket_size_msec=10)))
        self.assertEqual(10, len(rts.range(1, 0, 500, count=10)))
        self.assertEqual(2, len(rts.range(1, 0, 500, filter_by_ts=[i for i in range(10, 20)], filter_by_min_value=1,
                                          filter_by_max_value=2)))
        self.assertEqual([(0, 10.0), (10, 1.0)],
                         rts.range(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align='+'))
        self.assertEqual([(-5, 5.0), (5, 6.0)],
                         rts.range(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align=5))

    def testRevRange(self):
        '''Test TS.REVRANGE calls which returns reverse range by key'''
        # TS.REVRANGE is available since RedisTimeSeries >= v1.4
        if version is None or version < 14000:
            return

        for i in range(100):
            rts.add(1, i, i % 7)
        self.assertEqual(100, len(rts.range(1, 0, 200)))
        for i in range(100):
            rts.add(1, i + 200, i % 7)
        self.assertEqual(200, len(rts.range(1, 0, 500)))
        # first sample isn't returned
        self.assertEqual(20, len(rts.revrange(1, 0, 500, aggregation_type='avg', bucket_size_msec=10)))
        self.assertEqual(10, len(rts.revrange(1, 0, 500, count=10)))
        self.assertEqual(2, len(rts.revrange(1, 0, 500, filter_by_ts=[i for i in range(10, 20)], filter_by_min_value=1,
                                             filter_by_max_value=2)))
        self.assertEqual([(10, 1.0), (0, 10.0)],
                         rts.revrange(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align='+'))
        self.assertEqual([(1, 10.0), (-9, 1.0)],
                         rts.revrange(1, 0, 10, aggregation_type='count', bucket_size_msec=10, align=1))

    def testMultiRange(self):
        '''Test TS.MRANGE calls which returns range by filter'''

        rts.create(1, labels={'Test': 'This', 'team': 'ny'})
        rts.create(2, labels={'Test': 'This', 'Taste': 'That', 'team': 'sf'})
        for i in range(100):
            rts.add(1, i, i % 7)
            rts.add(2, i, i % 11)

        res = rts.mrange(0, 200, filters=['Test=This'])
        self.assertEqual(2, len(res))
        self.assertEqual(100, len(res[0]['1'][1]))

        res = rts.mrange(0, 200, filters=['Test=This'], count=10)
        self.assertEqual(10, len(res[0]['1'][1]))

        for i in range(100):
            rts.add(1, i + 200, i % 7)
        res = rts.mrange(0, 500, filters=['Test=This'],
                         aggregation_type='avg', bucket_size_msec=10)
        self.assertEqual(2, len(res))
        self.assertEqual(20, len(res[0]['1'][1]))

        # test withlabels
        self.assertEqual({}, res[0]['1'][0])
        res = rts.mrange(0, 200, filters=['Test=This'], with_labels=True)
        self.assertEqual({'Test': 'This', 'team': 'ny'}, res[0]['1'][0])
        # test with selected labels
        res = rts.mrange(0, 200, filters=['Test=This'], select_labels=['team'])
        self.assertEqual({'team': 'ny'}, res[0]['1'][0])
        self.assertEqual({'team': 'sf'}, res[1]['2'][0])
        # test with filterby
        res = rts.mrange(0, 200, filters=['Test=This'], filter_by_ts=[i for i in range(10, 20)],
                         filter_by_min_value=1, filter_by_max_value=2)
        self.assertEqual([(15, 1.0), (16, 2.0)], res[0]['1'][1])
        # test groupby
        res = rts.mrange(0, 3, filters=['Test=This'], groupby='Test', reduce='sum')
        self.assertEqual([(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)], res[0]['Test=This'][1])
        res = rts.mrange(0, 3, filters=['Test=This'], groupby='Test', reduce='max')
        self.assertEqual([(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)], res[0]['Test=This'][1])
        res = rts.mrange(0, 3, filters=['Test=This'], groupby='team', reduce='min')
        self.assertEqual(2, len(res))
        self.assertEqual([(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)], res[0]['team=ny'][1])
        self.assertEqual([(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)], res[1]['team=sf'][1])
        # test align
        res = rts.mrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align='-')
        self.assertEqual([(0, 10.0), (10, 1.0)], res[0]['1'][1])
        res = rts.mrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align=5)
        self.assertEqual([(-5, 5.0), (5, 6.0)], res[0]['1'][1])

    def testMultiReverseRange(self):
        '''Test TS.MREVRANGE calls which returns range by filter'''
        # TS.MREVRANGE is available since RedisTimeSeries >= v1.4
        if version is None or version < 14000:
            return

        rts.create(1, labels={'Test': 'This', 'team': 'ny'})
        rts.create(2, labels={'Test': 'This', 'Taste': 'That', 'team': 'sf'})
        for i in range(100):
            rts.add(1, i, i % 7)
            rts.add(2, i, i % 11)

        res = rts.mrange(0, 200, filters=['Test=This'])
        self.assertEqual(2, len(res))
        self.assertEqual(100, len(res[0]['1'][1]))

        res = rts.mrange(0, 200, filters=['Test=This'], count=10)
        self.assertEqual(10, len(res[0]['1'][1]))

        for i in range(100):
            rts.add(1, i + 200, i % 7)
        res = rts.mrevrange(0, 500, filters=['Test=This'],
                            aggregation_type='avg', bucket_size_msec=10)
        self.assertEqual(2, len(res))
        self.assertEqual(20, len(res[0]['1'][1]))

        # test withlabels
        self.assertEqual({}, res[0]['1'][0])
        res = rts.mrevrange(0, 200, filters=['Test=This'], with_labels=True)
        self.assertEqual({'Test': 'This', 'team': 'ny'}, res[0]['1'][0])
        # test with selected labels
        res = rts.mrevrange(0, 200, filters=['Test=This'], select_labels=['team'])
        self.assertEqual({'team': 'ny'}, res[0]['1'][0])
        self.assertEqual({'team': 'sf'}, res[1]['2'][0])
        # test filterby
        res = rts.mrevrange(0, 200, filters=['Test=This'], filter_by_ts=[i for i in range(10, 20)],
                            filter_by_min_value=1, filter_by_max_value=2)
        self.assertEqual([(16, 2.0), (15, 1.0)], res[0]['1'][1])
        # test groupby
        res = rts.mrevrange(0, 3, filters=['Test=This'], groupby='Test', reduce='sum')
        self.assertEqual([(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)], res[0]['Test=This'][1])
        res = rts.mrevrange(0, 3, filters=['Test=This'], groupby='Test', reduce='max')
        self.assertEqual([(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)], res[0]['Test=This'][1])
        res = rts.mrevrange(0, 3, filters=['Test=This'], groupby='team', reduce='min')
        self.assertEqual(2, len(res))
        self.assertEqual([(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)], res[0]['team=ny'][1])
        self.assertEqual([(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)], res[1]['team=sf'][1])
        # test align
        res = rts.mrevrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align='-')
        self.assertEqual([(10, 1.0), (0, 10.0)], res[0]['1'][1])
        res = rts.mrevrange(0, 10, filters=['team=ny'], aggregation_type='count', bucket_size_msec=10, align=1)
        self.assertEqual([(1, 10.0), (-9, 1.0)], res[0]['1'][1])

    def testGet(self):
        '''Test TS.GET calls'''

        name = 'test'
        rts.create(name)
        self.assertEqual(None, rts.get(name))
        rts.add(name, 2, 3)
        self.assertEqual(2, rts.get(name)[0])
        rts.add(name, 3, 4)
        self.assertEqual(4, rts.get(name)[1])

    def testMGet(self):
        '''Test TS.MGET calls'''
        rts.create(1, labels={'Test': 'This'})
        rts.create(2, labels={'Test': 'This', 'Taste': 'That'})
        act_res = rts.mget(['Test=This'])
        exp_res = [{'1': [{}, None, None]}, {'2': [{}, None, None]}]
        self.assertEqual(act_res, exp_res)
        rts.add(1, '*', 15)
        rts.add(2, '*', 25)
        res = rts.mget(['Test=This'])
        self.assertEqual(15, res[0]['1'][2])
        self.assertEqual(25, res[1]['2'][2])
        res = rts.mget(['Taste=That'])
        self.assertEqual(25, res[0]['2'][2])

        # test with_labels
        self.assertEqual({}, res[0]['2'][0])
        res = rts.mget(['Taste=That'], with_labels=True)
        self.assertEqual({'Taste': 'That', 'Test': 'This'}, res[0]['2'][0])

    def testInfo(self):
        '''Test TS.INFO calls'''
        rts.create(1, retention_msecs=5, labels={'currentLabel': 'currentData'})
        info = rts.info(1)
        self.assertEqual(5, info.retention_msecs)
        self.assertEqual(info.labels['currentLabel'], 'currentData')
        if version is None or version < 14000:
            return
        self.assertEqual(None, info.duplicate_policy)

        rts.create('time-serie-2', duplicate_policy='min')
        info = rts.info('time-serie-2')
        self.assertEqual('min', info.duplicate_policy)

    def testQueryIndex(self):
        '''Test TS.QUERYINDEX calls'''
        rts.create(1, labels={'Test': 'This'})
        rts.create(2, labels={'Test': 'This', 'Taste': 'That'})
        self.assertEqual(2, len(rts.queryindex(['Test=This'])))
        self.assertEqual(1, len(rts.queryindex(['Taste=That'])))
        self.assertEqual(['2'], rts.queryindex(['Taste=That']))

    def testPipeline(self):
        '''Test pipeline'''
        pipeline = rts.pipeline()
        pipeline.create('with_pipeline')
        for i in range(100):
            pipeline.add('with_pipeline', i, 1.1 * i)
        pipeline.execute()

        info = rts.info('with_pipeline')
        self.assertEqual(info.lastTimeStamp, 99)
        self.assertEqual(info.total_samples, 100)
        self.assertEqual(rts.get('with_pipeline')[1], 99 * 1.1)

    def testUncompressed(self):
        '''Test uncompressed chunks'''
        rts.create('compressed')
        rts.create('uncompressed', uncompressed=True)
        compressed_info = rts.info('compressed')
        uncompressed_info = rts.info('uncompressed')
        self.assertNotEqual(compressed_info.memory_usage, uncompressed_info.memory_usage)

    def testPool(self):
        redis = Redis(port=port)
        client = RedisTimeSeries(conn=redis, port=666)

        name = 'test'
        client.create(name)
        self.assertEqual(None, client.get(name))
        client.add(name, 2, 3)
        self.assertEqual(2, client.get(name)[0])
        info = client.info(name)
        self.assertEqual(1, info.total_samples)


if __name__ == '__main__':
    unittest.main()
