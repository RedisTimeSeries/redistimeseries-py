import six
import redis
from redis import Redis,  RedisError 
'''Redis = StrictRedis from redis-py 3.0'''
from redis.client import Pipeline, parse_info, bool_ok
from redis.connection import Token
from redis._compat import (long, nativestr)
from redis.exceptions import DataError

def parse_range(response):
    return response

def parse_m_range(response):
    return response

class Client(Redis): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements 
    RedisTimeSeries's commmands (prefixed with "ts").
    The client allows to interact with RedisTimeSeriRedisTimeSerieses and use all of
    it's functionality.
    """

    MODULE_INFO = {
        'name': 'RedisTimeSeries',
        'ver':  '0.1.0'
    }

    def __init__(self, *args, **kwargs):
        """
        Creates a new RedisTimeSeries client.
        """
        Redis.__init__(self, *args, **kwargs)
            
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            'TS.CREATE' : lambda r: r and nativestr(r),
            'TS.ADD': bool_ok,
            'TS.INCRBY': bool_ok,
            'TS.DECRBY': bool_ok,
            'TS.CREATERULE': bool_ok,
            'TS.DELETERULE':bool_ok,
            'TS.RANGE': parse_range,
            'TS.MRANGE': parse_m_range,
            #'TS.GET': , response is enough
            #'TS.INFO': parse_info, # TODO
        }
        for k, v in six.iteritems(MODULE_CALLBACKS):
            self.set_response_callback(k, v)

    AGGREGATIONS = [None, 'avg', 'sum', 'min', 'max', 
                    'range', 'count', 'first', 'last']

    @staticmethod
    def appendRetention(params, retention):
        if retention is not None:
            params.extend(['RETENTION', retention])
            
    @staticmethod
    def appendTimeBucket(params, timeBucket):
        if timeBucket is not None:
            params.extend(['RESET', timeBucket])

    @staticmethod
    def appendLabels(params, **labels):
        if labels:
            params.append('LABELS')
            [params.extend([k,v]) for k,v in labels.items()]

    def appendAggregation(self, params, aggregationType, 
                          bucketSizeSeconds):
        if aggregationType not in self.AGGREGATIONS:
            raise DataError('Aggregation type is invalid')
        else:
            params.append('AGGREGATION')
            params.extend([aggregationType, bucketSizeSeconds])

    def tsCreate(self, key, retentionSecs=None, labels={}):
        """
        Creates a new time-series ``key`` with ``rententionSecs`` in 
        seconds and ``labels``.
        """
        params = [key]
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, **labels)

        return self.execute_command('TS.CREATE', *params)
        
    def tsAdd(self, key, timestamp, value, 
              retentionSecs=None, labels={}):
        """
        Appends (or creates and appends) a new ``value`` to series 
        ``key`` with ``timestamp``. If ``key`` is created, 
        ``retentionSecs`` and ``labels`` are applied.
        """
        params = [key, timestamp, value]
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, **labels)

        return self.execute_command('TS.ADD', *params)

    def tsIncreaseBy(self, key, value, timeBucket=None,
                     retentionSecs=None, labels={}): 
        """
        Increces latest value in ``key`` by ``value``.
        ``timeBucket`` resets counter. In seconds.
        If ``key`` is created, ``retentionSecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimeBucket(params, timeBucket)
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, **labels)

        return self.execute_command('TS.INCRBY', *params)

    def tsDecreaseBy(self, key, value, timeBucket=None,
                     retentionSecs=None, labels={}):  
        """
        Decreases latest value in ``key`` by ``value``.
        ``timeBucket`` resets counter. In seconds.
        If ``key`` is created, ``retentionSecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimeBucket(params, timeBucket)
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, **labels)
        
        return self.execute_command('TS.INCRBY', *params)

    def tsCreateRule(self, sourceKey, destKey, 
                     aggregationType, bucketSizeSeconds):
        """
        Creates a compaction rule from values added to ``sourceKey`` 
        into ``destKey``. Aggregating for bucketSizeSeconds where an
        ``aggregationType`` can be ['avg', 'sum', 'min', 'max',
        'range', 'count', 'first', 'last']
        """
        params=[sourceKey, destKey]
        self.appendAggregation(params, aggregationType, bucketSizeSeconds)

        return self.execute_command('TS.CREATERULE', *params)

    def tsDeleteRule(self, sourceKey, destKey):
        """Deletes a compaction rule"""
        return self.execute_command('TS.DELETERULE', sourceKey, destKey)
   
    def tsRange(self, key, fromTime, toTime, 
                aggregationType=None, bucketSizeSeconds=0):
        """
        Query a range from ``key``, from ``fromTime`` to ``toTime``.
        Can Aggregate for bucketSizeSeconds where an ``aggregationType``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [key, fromTime, toTime]
        if aggregationType != None:
            self.appendAggregation(params, aggregationType, bucketSizeSeconds)

        return self.execute_command('TS.RANGE', *params)

#TODO - filters shouldn't have default
    def tsMultiRange(self, fromTime, toTime, filters=[],
                     aggregationType=None, bucketSizeSeconds=0):
        """
        Query a range based on filters,retentionSecs from ``fromTime`` to ``toTime``.
        Can Aggregate for bucketSizeSeconds where an ``aggregationType``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [fromTime, toTime]
        if aggregationType != None:
            self.appendAggregation(params, aggregationType, bucketSizeSeconds)
        params.extend(['FILTER', *filters])
        return self.execute_command('TS.MRANGE', *params)

    def tsGet(self, key):
        """Gets the last sample of ``key``"""
        return self.execute_command('TS.GET', key)

    def tsInfo(self, key):
        """Gets information of ``key``"""
        return self.execute_command('TS.INFO', key)
