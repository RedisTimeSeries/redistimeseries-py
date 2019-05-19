import six
import redis
from redis import Redis, RedisError 
from redis.client import bool_ok
from redis._compat import (long, nativestr)
from redis.exceptions import DataError

class TSInfo(object):
    chunkCount = None
    labels = []
    lastTimeStamp = None
    maxSamplesPerChunk = None
    retentionSecs = None
    rules = []

    def __init__(self, args):
        self.chunkCount = args['chunkCount']
        self.labels = list_to_dict(args['labels'])
        self.lastTimeStamp = args['lastTimestamp']
        self.maxSamplesPerChunk = args['maxSamplesPerChunk']
        self.retentionSecs = args['retentionSecs']
        self.rules = args['rules']

def list_to_dict(aList):
    return {nativestr(aList[i][0]):nativestr(aList[i][1])
                for i in range(len(aList))}

def parse_range(response):
    return [tuple((l[0], l[1].decode())) for l in response]

def parse_m_range(response):
    res = []
    for item in response:
        res.append({ nativestr(item[0]) : [list_to_dict(item[1]), 
                                parse_range(item[2])]})
    return res

def parse_info(response):
    res = dict(zip(map(nativestr, response[::2]), response[1::2]))
    info = TSInfo(res)
    return info   

class Client(Redis): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements 
    RedisTimeSeries's commmands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of
    it's functionality.
    """

    MODULE_INFO = {
        'name': 'RedisTimeSeries',
        'ver':  '0.1.0'
    }

    CREATE_CMD = 'TS.CREATE'
    ADD_CMD = 'TS.ADD'
    INCRBY_CMD = 'TS.INCRBY'
    DECRBY_CMD = 'TS.DECRBY'
    CREATERULE_CMD = 'TS.CREATERULE'
    DELETERULE_CMD = 'TS.DELETERULE'
    RANGE_CMD = 'TS.RANGE'
    MRANGE_CMD = 'TS.MRANGE'
    GET_CMD = 'TS.GET'
    INFO_CMD = 'TS.INFO'

    def __init__(self, *args, **kwargs):
        """
        Creates a new RedisTimeSeries client.
        """
        Redis.__init__(self, *args, **kwargs)
            
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.CREATE_CMD : bool_ok,
            self.ADD_CMD : bool_ok,
            self.INCRBY_CMD : bool_ok,
            self.DECRBY_CMD : bool_ok,
            self.CREATERULE_CMD : bool_ok,
            self.DELETERULE_CMD : bool_ok,
            self.RANGE_CMD : parse_range,
            self.MRANGE_CMD : parse_m_range,
            self.GET_CMD : lambda x: (int(x[0]), float(x[1])),
            self.INFO_CMD : parse_info,
        }
        for k, v in six.iteritems(MODULE_CALLBACKS):
            self.set_response_callback(k, v)

    @staticmethod
    def appendRetention(params, retention):
        if retention is not None:
            params.extend(['RETENTION', retention])
            
    @staticmethod
    def appendTimeBucket(params, timeBucket):
        if timeBucket is not None:
            params.extend(['RESET', timeBucket])

    @staticmethod
    def appendLabels(params, labels):
        if labels:
            params.append('LABELS')
            for k, v in labels.items():
                params.extend([k,v])

    @staticmethod
    def appendAggregation(params, aggregationType, 
                          bucketSizeSeconds):     
        params.append('AGGREGATION')
        params.extend([aggregationType, bucketSizeSeconds])

    def create(self, key, retentionSecs=None, labels={}):
        """
        Creates a new time-series ``key`` with ``rententionSecs`` in 
        seconds and ``labels``.
        """
        params = [key]
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.CREATE_CMD, *params)
        
    def add(self, key, timestamp, value, 
              retentionSecs=None, labels={}):
        """
        Appends (or creates and appends) a new ``value`` to series 
        ``key`` with ``timestamp``. If ``key`` is created, 
        ``retentionSecs`` and ``labels`` are applied.
        """
        params = [key, timestamp, value]
        self.appendRetention(params, retentionSecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.ADD_CMD, *params)

    def incrby(self, key, value, timeBucket=None,
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
        self.appendLabels(params, labels)

        return self.execute_command(self.INCRBY_CMD, *params)

    def decrby(self, key, value, timeBucket=None,
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
        self.appendLabels(params, labels)
        
        return self.execute_command(self.DECRBY_CMD, *params)

    def createrule(self, sourceKey, destKey, 
                     aggregationType, bucketSizeSeconds):
        """
        Creates a compaction rule from values added to ``sourceKey`` 
        into ``destKey``. Aggregating for ``bucketSizeSeconds`` where an
        ``aggregationType`` can be ['avg', 'sum', 'min', 'max',
        'range', 'count', 'first', 'last']
        """
        params=[sourceKey, destKey]
        self.appendAggregation(params, aggregationType, bucketSizeSeconds)

        return self.execute_command(self.CREATERULE_CMD, *params)

    def deleterule(self, sourceKey, destKey):
        """Deletes a compaction rule"""
        return self.execute_command(self.DELETERULE_CMD, sourceKey, destKey)
   
    def range(self, key, fromTime, toTime, 
                aggregationType=None, bucketSizeSeconds=0):
        """
        Query a range from ``key``, from ``fromTime`` to ``toTime``.
        Can Aggregate for ``bucketSizeSeconds`` where an ``aggregationType``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [key, fromTime, toTime]
        if aggregationType != None:
            self.appendAggregation(params, aggregationType, bucketSizeSeconds)

        return self.execute_command(self.RANGE_CMD, *params)

    def mrange(self, fromTime, toTime, filters,
                     aggregationType=None, bucketSizeSeconds=0):
        """
        Query a range based on filters,retentionSecs from ``fromTime`` to ``toTime``.
        ``filters`` are a list strings such as ['Test=This'].
        Can Aggregate for ``bucketSizeSeconds`` where an ``aggregationType``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [fromTime, toTime]
        if aggregationType != None:
            self.appendAggregation(params, aggregationType, bucketSizeSeconds)
        params.extend(['FILTER'])
        params += filters
        return self.execute_command(self.MRANGE_CMD, *params)

    def get(self, key):
        """Gets the last sample of ``key``"""
        return self.execute_command(self.GET_CMD, key)

    def info(self, key):
        """Gets information of ``key``"""
        return self.execute_command(self.INFO_CMD, key)
