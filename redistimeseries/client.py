from redis import Redis
from redis.client import Pipeline
from redis.client import bool_ok
from redis._compat import nativestr

class TSInfo(object):
    rules = []
    labels = []
    sourceKey = None
    chunk_count = None
    memory_usage = None
    total_samples = None
    retention_msecs = None
    last_time_stamp = None
    first_time_stamp = None
    # As of RedisTimeseries >= v1.4 max_samples_per_chunk is deprecated in favor of chunk_size
    max_samples_per_chunk = None
    chunk_size = None


    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.rules = response['rules']
        self.sourceKey = response['sourceKey']
        self.chunkCount = response['chunkCount']
        self.memory_usage = response['memoryUsage']
        self.total_samples = response['totalSamples']
        self.labels = list_to_dict(response['labels'])
        self.retention_msecs = response['retentionTime']
        self.lastTimeStamp = response['lastTimestamp']
        self.first_time_stamp = response['firstTimestamp']
        if 'maxSamplesPerChunk' in response:
            self.max_samples_per_chunk = response['maxSamplesPerChunk']
            self.chunk_size = self.max_samples_per_chunk * 16 # backward compatible changes
        if 'chunkSize' in response:
            self.chunkSize = response['chunkSize']

def list_to_dict(aList):
    return {nativestr(aList[i][0]):nativestr(aList[i][1])
                for i in range(len(aList))}

def parse_range(response):
    return [tuple((l[0], float(l[1]))) for l in response]

def parse_m_range(response):
    res = []
    for item in response:
        res.append({ nativestr(item[0]) : [list_to_dict(item[1]),
                                parse_range(item[2])]})
    return res

def parse_get(response):
    if response == []:
        return None
    return (int(response[0]), float(response[1]))

def parse_m_get(response):
    res = []
    for item in response:
        if item[2] == []:
            res.append({ nativestr(item[0]) : [list_to_dict(item[1]), None, None]})
        else:
            res.append({ nativestr(item[0]) : [list_to_dict(item[1]),
                                int(item[2][0]), float(item[2][1])]})

    return res

def parseToList(response):
    res = []
    for item in response:
        res.append(nativestr(item))
    return res

class Client(object): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements
    RedisTimeSeries's commands (prefixed with "ts").
    The client allows to interact with RedisTimeSeries and use all of
    it's functionality.
    """

    CREATE_CMD = 'TS.CREATE'
    ALTER_CMD = 'TS.ALTER'
    ADD_CMD = 'TS.ADD'
    MADD_CMD = 'TS.MADD'
    INCRBY_CMD = 'TS.INCRBY'
    DECRBY_CMD = 'TS.DECRBY'
    CREATERULE_CMD = 'TS.CREATERULE'
    DELETERULE_CMD = 'TS.DELETERULE'
    RANGE_CMD = 'TS.RANGE'
    REVRANGE_CMD = 'TS.REVRANGE'
    MRANGE_CMD = 'TS.MRANGE'
    MREVRANGE_CMD = 'TS.MREVRANGE'
    GET_CMD = 'TS.GET'
    MGET_CMD = 'TS.MGET'
    INFO_CMD = 'TS.INFO'
    QUERYINDEX_CMD = 'TS.QUERYINDEX'

    def __init__(self, conn=None, *args, **kwargs):
        """
        Creates a new RedisTimeSeries client.
        """
        self.redis = conn if conn is not None else Redis(*args, **kwargs)

        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.CREATE_CMD : bool_ok,
            self.ALTER_CMD : bool_ok,
            self.CREATERULE_CMD : bool_ok,
            self.DELETERULE_CMD : bool_ok,
            self.RANGE_CMD : parse_range,
            self.REVRANGE_CMD: parse_range,
            self.MRANGE_CMD : parse_m_range,
            self.MREVRANGE_CMD: parse_m_range,
            self.GET_CMD : parse_get,
            self.MGET_CMD : parse_m_get,
            self.INFO_CMD : TSInfo,
            self.QUERYINDEX_CMD : parseToList,
        }
        for k in MODULE_CALLBACKS:
            self.redis.set_response_callback(k, MODULE_CALLBACKS[k])

    @staticmethod
    def appendUncompressed(params, uncompressed):
        if uncompressed:
            params.extend(['UNCOMPRESSED'])

    @staticmethod
    def appendWithLabels(params, with_labels):
        if with_labels:
            params.extend(['WITHLABELS'])

    @staticmethod
    def appendRetention(params, retention):
        if retention is not None:
            params.extend(['RETENTION', retention])

    @staticmethod
    def appendLabels(params, labels):
        if labels:
            params.append('LABELS')
            for k, v in labels.items():
                params.extend([k,v])

    @staticmethod
    def appendCount(params, count):
        if count is not None:
            params.extend(['COUNT', count])

    @staticmethod
    def appendTimestamp(params, timestamp):
        if timestamp is not None:
            params.extend(['TIMESTAMP', timestamp])

    @staticmethod
    def appendAggregation(params, aggregation_type,
                          bucket_size_msec):
        params.append('AGGREGATION')
        params.extend([aggregation_type, bucket_size_msec])

    def create(self, key, retention_msecs=None, uncompressed=False, labels={}):
        """
        Creates a new time-series ``key`` with ``retention_msecs`` in
        milliseconds and ``labels``.
        """
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendLabels(params, labels)

        return self.redis.execute_command(self.CREATE_CMD, *params)
        
    def alter(self, key, retention_msecs=None, labels={}):
        """
        Update the retention, labels of an existing key. The parameters
        are the same as TS.CREATE.
        """
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendLabels(params, labels)

        return self.redis.execute_command(self.ALTER_CMD, *params)

    def add(self, key, timestamp, value, retention_msecs=None,
                        uncompressed=False, labels={}):
        """
        Appends (or creates and appends) a new ``value`` to series
        ``key`` with ``timestamp``. If ``key`` is created, 
        ``retention_msecs`` and ``labels`` are applied. Return value
        is timestamp of insertion.
        """
        params = [key, timestamp, value]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendLabels(params, labels)

        return self.redis.execute_command(self.ADD_CMD, *params)

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

        return self.redis.execute_command(self.MADD_CMD, *params)

    def incrby(self, key, value, timestamp=None, retention_msecs=None,
                uncompressed=False, labels={}):
        """
        Increases latest value in ``key`` by ``value``.
        ``timestamp`` can be set or system time will be used.
        If ``key`` is created, ``retention_msecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendLabels(params, labels)

        return self.redis.execute_command(self.INCRBY_CMD, *params)

    def decrby(self, key, value, timestamp=None, retention_msecs=None,
                uncompressed=False, labels={}):
        """
        Decreases latest value in ``key`` by ``value``.
        ``timestamp` can be set or system time will be used.
        If ``key`` is created, ``retention_msecs`` and ``labels`` are
        applied. 
        """
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendLabels(params, labels)
        
        return self.redis.execute_command(self.DECRBY_CMD, *params)

    def createrule(self, source_key, dest_key, 
                     aggregation_type, bucket_size_msec):
        """
        Creates a compaction rule from values added to ``source_key``
        into ``dest_key``. Aggregating for ``bucket_size_msec`` where an
        ``aggregation_type`` can be ['avg', 'sum', 'min', 'max',
        'range', 'count', 'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
        """
        params=[source_key, dest_key]
        self.appendAggregation(params, aggregation_type, bucket_size_msec)

        return self.redis.execute_command(self.CREATERULE_CMD, *params)

    def deleterule(self, source_key, dest_key):
        """Deletes a compaction rule"""
        return self.redis.execute_command(self.DELETERULE_CMD, source_key, dest_key)

    def __range_params(self, key, from_time, to_time, count, aggregation_type, bucket_size_msec):
        """
        Internal method to create TS.RANGE and TS.REVRANGE arguments
        """
        params = [key, from_time, to_time]
        self.appendCount(params, count)
        if aggregation_type is not None:
            self.appendAggregation(params, aggregation_type, bucket_size_msec)
        return params

    def range(self, key, from_time, to_time, count=None,
                aggregation_type=None, bucket_size_msec=0):
        """
        Query a range in forward direction for a specific time-serie.

        Args:
            key: Key name for timeseries.
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
        """
        params = self.__range_params(key, from_time, to_time, count, aggregation_type, bucket_size_msec)
        return self.redis.execute_command(self.RANGE_CMD, *params)

    def revrange(self, key, from_time, to_time, count=None,
              aggregation_type=None, bucket_size_msec=0):
        """
        Query a range in reverse direction for a specific time-serie.
        Note: This command is only available since RedisTimeSeries >= v1.4

        Args:
            key: Key name for timeseries.
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
        """
        params = self.__range_params(key, from_time, to_time, count, aggregation_type, bucket_size_msec)
        return self.redis.execute_command(self.REVRANGE_CMD, *params)


    def __mrange_params(self, aggregation_type, bucket_size_msec, count, filters, from_time, to_time, with_labels):
        """
        Internal method to create TS.MRANGE and TS.MREVRANGE arguments
        """
        params = [from_time, to_time]
        self.appendCount(params, count)
        if aggregation_type is not None:
            self.appendAggregation(params, aggregation_type, bucket_size_msec)
        self.appendWithLabels(params, with_labels)
        params.extend(['FILTER'])
        params += filters
        return params

    def mrange(self, from_time, to_time, filters, count=None,
                     aggregation_type=None, bucket_size_msec=0, with_labels=False):
        """
        Query a range across multiple time-series by filters in forward direction.

        Args:
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            filters: filter to match the time-series labels.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            with_labels:  Include in the reply the label-value pairs that represent metadata labels of the time-series.
            If this argument is not set, by default, an empty Array will be replied on the labels array position.
        """
        params = self.__mrange_params(aggregation_type, bucket_size_msec, count, filters, from_time, to_time, with_labels)
        return self.redis.execute_command(self.MRANGE_CMD, *params)

    def mrevrange(self, from_time, to_time, filters, count=None,
                     aggregation_type=None, bucket_size_msec=0, with_labels=False):
        """
        Query a range across multiple time-series by filters in reverse direction.

        Args:
            from_time: Start timestamp for the range query. - can be used to express the minimum possible timestamp (0).
            to_time:  End timestamp for range query, + can be used to express the maximum possible timestamp.
            filters: filter to match the time-series labels.
            count: Optional maximum number of returned results.
            aggregation_type: Optional aggregation type. Can be one of ['avg', 'sum', 'min', 'max', 'range', 'count', 'first',
        'last', 'std.p', 'std.s', 'var.p', 'var.s']
            bucket_size_msec: Time bucket for aggregation in milliseconds.
            with_labels:  Include in the reply the label-value pairs that represent metadata labels of the time-series.
            If this argument is not set, by default, an empty Array will be replied on the labels array position.
        """
        params = self.__mrange_params(aggregation_type, bucket_size_msec, count, filters, from_time, to_time, with_labels)
        return self.redis.execute_command(self.MREVRANGE_CMD, *params)

    def get(self, key):
        """Gets the last sample of ``key``"""
        return self.redis.execute_command(self.GET_CMD, key)

    def mget(self, filters, with_labels=False):
        """Get the last samples matching the specific ``filter``."""
        params = []
        self.appendWithLabels(params, with_labels)
        params.extend(['FILTER'])
        params += filters
        return self.redis.execute_command(self.MGET_CMD, *params)
   
    def info(self, key):
        """Gets information of ``key``"""
        return self.redis.execute_command(self.INFO_CMD, key)

    def queryindex(self, filters):
        """Get all the keys matching the ``filter`` list."""
        return self.redis.execute_command(self.QUERYINDEX_CMD, *filters)

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.redis.connection_pool,
            response_callbacks=self.redis.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint)
        p.redis = p
        return p

class Pipeline(Pipeline, Client):
    "Pipeline for Redis TimeSeries Client"
