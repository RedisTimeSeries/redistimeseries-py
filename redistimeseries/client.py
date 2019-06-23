import six
import redis
from redis import Redis, RedisError 
from redis.client import bool_ok
from redis.client import int_or_none
from redis._compat import (long, nativestr)
from redis.exceptions import DataError

class TSInfo(object):
    chunk_count = None
    labels = []
    last_time_stamp = None
    max_samples_per_chunk = None
    retention_msecs = None
    rules = []

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.chunkCount = response['chunkCount']
        self.labels = list_to_dict(response['labels'])
        self.lastTimeStamp = response['lastTimestamp']
        self.maxSamplesPerChunk = response['maxSamplesPerChunk']
        self.retention_msecs = response['retentionTime']
        self.rules = response['rules']

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

def parse_m_get(response):
    res = []
    for item in response:
        res.append({ nativestr(item[0]) : [list_to_dict(item[1]), 
                                item[2], nativestr(item[3])]})
    return res
    
def parseToList(response):
    res = []
    for item in response:
        res.append(nativestr(item))
    return res

class Client(Redis): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements 
    RedisTimeSeries's commands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of
    it's functionality.
    """

    MODULE_INFO = {
        'name': 'RedisTimeSeries',
        'ver':  '0.1.0'
    }

    CREATE_CMD = 'TS.CREATE'
    ALTER_CMD = 'TS.ALTER'
    ADD_CMD = 'TS.ADD'
    MADD_CMD = 'TS.MADD'
    INCRBY_CMD = 'TS.INCRBY'
    DECRBY_CMD = 'TS.DECRBY'
    CREATERULE_CMD = 'TS.CREATERULE'
    DELETERULE_CMD = 'TS.DELETERULE'
    RANGE_CMD = 'TS.RANGE'
    MRANGE_CMD = 'TS.MRANGE'
    GET_CMD = 'TS.GET'
    MGET_CMD = 'TS.MGET'
    INFO_CMD = 'TS.INFO'
    QUERYINDEX_CMD = 'TS.QUERYINDEX'

    def __init__(self, *args, **kwargs):
        """
        Creates a new RedisTimeSeries client.
        """
        Redis.__init__(self, *args, **kwargs)
            
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.CREATE_CMD : bool_ok,
            self.ALTER_CMD : bool_ok, 
            #self.ADD_CMD : bool_ok,
            #self.MADD_CMD : bool_ok,
            self.INCRBY_CMD : bool_ok,
            self.DECRBY_CMD : bool_ok,
            self.CREATERULE_CMD : bool_ok,
            self.DELETERULE_CMD : bool_ok,
            self.RANGE_CMD : parse_range,
            self.MRANGE_CMD : parse_m_range,
            self.GET_CMD : lambda x: (int(x[0]), float(x[1])),
            self.MGET_CMD : parse_m_get,
            self.INFO_CMD : TSInfo,
            self.QUERYINDEX_CMD : parseToList,
        }
        for k, v in six.iteritems(MODULE_CALLBACKS):
            self.set_response_callback(k, v)

    @staticmethod
    def appendRetention(params, retention):
        if retention is not None:
            params.extend(['RETENTION', retention])
            
    @staticmethod
    def appendTimeBucket(params, time_bucket):
        if time_bucket is not None:
            params.extend(['RESET', time_bucket])

    @staticmethod
    def appendLabels(params, labels):
        if labels:
            params.append('LABELS')
            for k, v in labels.items():
                params.extend([k,v])

    @staticmethod
    def appendAggregation(params, aggregation_type, 
                          bucket_size_msec):     
        params.append('AGGREGATION')
        params.extend([aggregation_type, bucket_size_msec])

    def create(self, key, retention_msecs=None, labels={}):
        """
        Creates a new time-series ``key`` with ``retention_msecs`` in 
        milliseconds and ``labels``.
        """
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.CREATE_CMD, *params)
        
    def alter(self, key, retention_msecs=None, labels={}):
        """
        Update the retention, labels of an existing key. The parameters 
        are the same as TS.CREATE.
        """
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.ALTER_CMD, *params)

    def add(self, key, timestamp, value, 
              retention_msecs=None, labels={}):
        """
        Appends (or creates and appends) a new ``value`` to series 
        ``key`` with ``timestamp``. If ``key`` is created, 
        ``retention_msecs`` and ``labels`` are applied. Return value
        is timestamp of insertion.
        """
        params = [key, timestamp, value]
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.ADD_CMD, *params)

    def madd(self, ktv_tuples):
        """
        Appends (or creates and appends) a new ``value`` to series 
        ``key`` with ``timestamp``. Expects a list of ``tuples`` as
        (``key``,``timestamp``, ``value``). Return value is an
        array with timestamps of insertions.
        """
        params = []
        for ktv in ktv_tuples:
            for item in ktv:
                params.append(item)

        return self.execute_command(self.MADD_CMD, *params)

    def incrby(self, key, value, time_bucket=None,
                     retention_msecs=None, labels={}): 
        """
        Increases latest value in ``key`` by ``value``.
        ``timeBucket`` resets counter. In milliseconds.
        If ``key`` is created, ``retention_msecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimeBucket(params, time_bucket)
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)

        return self.execute_command(self.INCRBY_CMD, *params)

    def decrby(self, key, value, time_bucket=None,
                     retention_msecs=None, labels={}):  
        """
        Decreases latest value in ``key`` by ``value``.
        ``time_bucket`` resets counter. In milliseconds.
        If ``key`` is created, ``retention_msecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimeBucket(params, time_bucket)
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)
        
        return self.execute_command(self.DECRBY_CMD, *params)

    def createrule(self, source_key, dest_key, 
                     aggregation_type, bucket_size_msec):
        """
        Creates a compaction rule from values added to ``source_key`` 
        into ``dest_key``. Aggregating for ``bucket_size_msec`` where an
        ``aggregation_type`` can be ['avg', 'sum', 'min', 'max',
        'range', 'count', 'first', 'last']
        """
        params=[source_key, dest_key]
        self.appendAggregation(params, aggregation_type, bucket_size_msec)

        return self.execute_command(self.CREATERULE_CMD, *params)

    def deleterule(self, source_key, dest_key):
        """Deletes a compaction rule"""
        return self.execute_command(self.DELETERULE_CMD, source_key, dest_key)
   
    def range(self, key, from_time, to_time, 
                aggregation_type=None, bucket_size_msec=0):
        """
        Query a range from ``key``, from ``from_time`` to ``to_time``.
        Can Aggregate for ``bucket_size_msec`` where an ``aggregation_type``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [key, from_time, to_time]
        if aggregation_type != None:
            self.appendAggregation(params, aggregation_type, bucket_size_msec)

        return self.execute_command(self.RANGE_CMD, *params)

    def mrange(self, from_time, to_time, filters,
                     aggregation_type=None, bucket_size_msec=0):
        """
        Query a range based on filters,retention_msecs from ``from_time`` to ``to_time``.
        ``filters`` are a list strings such as ['Test=This'].
        Can Aggregate for ``bucket_size_msec`` where an ``aggregation_type``
        can be ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last']
        """
        params = [from_time, to_time]
        if aggregation_type != None:
            self.appendAggregation(params, aggregation_type, bucket_size_msec)
        params.extend(['FILTER'])
        params += filters
        return self.execute_command(self.MRANGE_CMD, *params)

    def get(self, key):
        """Gets the last sample of ``key``"""
        return self.execute_command(self.GET_CMD, key)

    def mget(self, filters):
        """Get the last samples matching the specific ``filter``."""
        params = ['FILTER']
        params += filters
        return self.execute_command(self.MGET_CMD, *params)
   
    def info(self, key):
        """Gets information of ``key``"""
        return self.execute_command(self.INFO_CMD, key)

    def queryindex(self, filters):
        """Get all the keys matching the ``filter`` list."""
        return self.execute_command(self.QUERYINDEX_CMD, *filters)