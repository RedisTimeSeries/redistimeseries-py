from aioredis import Redis

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
    duplicate_policy = None


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
            self.chunk_size = response['chunkSize']
        if 'duplicatePolicy' in response:
            self.duplicate_policy = response['duplicatePolicy']
            if type(self.duplicate_policy) == bytes:
                self.duplicate_policy = self.duplicate_policy.decode()

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

def bool_ok(response):
    return nativestr(response) == 'OK'

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
    redis:Redis=None
    @classmethod
    async def create(conn:Redis):
        """
        Creates a new RedisTimeSeries client.
        """
        self = Client()
        self.redis = conn

        # Set the module commands' callbacks
        self.MODULE_CALLBACKS = {
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
        # for k in MODULE_CALLBACKS:
        #     self.redis.set_response_callback(k, MODULE_CALLBACKS[k])

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

    @staticmethod
    def appendChunkSize(params, chunk_size):
        if chunk_size is not None:
            params.extend(['CHUNK_SIZE', chunk_size])

    @staticmethod
    def appendDuplicatePolicy(params, command, duplicate_policy):
        if duplicate_policy is not None:
            if command == 'TS.ADD':
                params.extend(['ON_DUPLICATE', duplicate_policy])
            else:
                params.extend(['DUPLICATE_POLICY', duplicate_policy])

    def _execute(self,cmd:str,response):
        if response is not None and cmd in self.MODULE_CALLBACKS:
            return self.MODULE_CALLBACKS[cmd](response)
        return response

    async def create(self, key, **kwargs):
        """
        Create a new time-series.

        Args:
            key: time-series key
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
            duplicate_policy: since RedisTimeSeries v1.4 you can specify the duplicate sample policy ( Configure what to do on duplicate sample. )
                        Can be one of:
                              - 'block': an error will occur for any out of order sample
                              - 'first': ignore the new value
                              - 'last': override with latest value
                              - 'min': only override if the value is lower than the existing value
                              - 'max': only override if the value is higher than the existing value
                        When this is not set, the server-wide default will be used.
        """
        retention_msecs = kwargs.get('retention_msecs', None)
        uncompressed = kwargs.get('uncompressed', False)
        labels = kwargs.get('labels', {})
        chunk_size = kwargs.get('chunk_size', None)
        duplicate_policy = kwargs.get('duplicate_policy', None)
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendDuplicatePolicy(params, self.CREATE_CMD, duplicate_policy)
        self.appendLabels(params, labels)

        return self._execute(self.CREATE_CMD,await self.redis.execute(self.CREATE_CMD, *params))
        
    async def alter(self, key, **kwargs):
        """
        Update the retention, labels of an existing key. The parameters
        are the same as TS.CREATE.
        """
        retention_msecs = kwargs.get('retention_msecs', None)
        labels = kwargs.get('labels', {})
        duplicate_policy = kwargs.get('duplicate_policy', None)
        params = [key]
        self.appendRetention(params, retention_msecs)
        self.appendDuplicatePolicy(params, self.ALTER_CMD, duplicate_policy)
        self.appendLabels(params, labels)

        return self._execute(self.ALTER_CMD,await self.redis.execute(self.ALTER_CMD, *params))
        # return await self.redis.execute(self.ALTER_CMD, *params)

    async def add(self, key, timestamp, value, **kwargs):
        """
        Append (or create and append) a new sample to the series.

        Args:
            key: time-series key
            timestamp: timestamp of the sample. * can be used for automatic timestamp (using the system clock).
            value: numeric data value of the sample
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
            duplicate_policy: since RedisTimeSeries v1.4 you can specify the duplicate sample policy ( Configure what to do on duplicate sample. )
                        Can be one of:
                              - 'block': an error will occur for any out of order sample
                              - 'first': ignore the new value
                              - 'last': override with latest value
                              - 'min': only override if the value is lower than the existing value
                              - 'max': only override if the value is higher than the existing value
                        When this is not set, the server-wide default will be used.
        """
        retention_msecs = kwargs.get('retention_msecs', None)
        uncompressed = kwargs.get('uncompressed', False)
        labels = kwargs.get('labels', {})
        chunk_size = kwargs.get('chunk_size', None)
        duplicate_policy = kwargs.get('duplicate_policy', None)
        params = [key, timestamp, value]
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendDuplicatePolicy(params, self.ADD_CMD, duplicate_policy)
        self.appendLabels(params, labels)
        return self._execute(self.ADD_CMD,await self.redis.execute(self.ADD_CMD, *params))
        # return await self.redis.execute(self.ADD_CMD, *params)

    async def madd(self, ktv_tuples):
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

        return self._execute(self.MADD_CMD,await self.redis.execute(self.MADD_CMD, *params))
        # return await self.redis.execute(self.MADD_CMD, *params)

    async def incrby(self, key, value, **kwargs):
        """
        Increment (or create an time-series and increment) the latest sample's of a series.
        This command can be used as a counter or gauge that automatically gets history as a time series.

        Args:
            key: time-series key
            value: numeric data value of the sample
            timestamp: timestamp of the sample. None can be used for automatic timestamp (using the system clock).
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
        """
        timestamp = kwargs.get('timestamp', None)
        retention_msecs = kwargs.get('retention_msecs', None)
        uncompressed = kwargs.get('uncompressed', False)
        labels = kwargs.get('labels', {})
        chunk_size = kwargs.get('chunk_size', None)
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendLabels(params, labels)
        return self._execute(self.INCRBY_CMD,await self.redis.execute(self.INCRBY_CMD, *params))
        # return self.redis.execute(self.INCRBY_CMD, *params)

    async def decrby(self, key, value, **kwargs):
        """
        Decrement (or create an time-series and decrement) the latest sample's of a series.
        This command can be used as a counter or gauge that automatically gets history as a time series.

        Args:
            key: time-series key
            value: numeric data value of the sample
            timestamp: timestamp of the sample. None can be used for automatic timestamp (using the system clock).
            retention_msecs: Maximum age for samples compared to last event time (in milliseconds).
                        If None or 0 is passed then  the series is not trimmed at all.
            uncompressed: since RedisTimeSeries v1.2, both timestamps and values are compressed by default.
                        Adding this flag will keep data in an uncompressed form. Compression not only saves
                        memory but usually improve performance due to lower number of memory accesses
            labels: Set of label-value pairs that represent metadata labels of the key.
            chunk_size: Each time-serie uses chunks of memory of fixed size for time series samples.
                        You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes).
        """
        timestamp = kwargs.get('timestamp', None)
        retention_msecs = kwargs.get('retention_msecs', None)
        uncompressed = kwargs.get('uncompressed', False)
        labels = kwargs.get('labels', {})
        chunk_size = kwargs.get('chunk_size', None)
        params = [key, value]
        self.appendTimestamp(params, timestamp)
        self.appendRetention(params, retention_msecs)
        self.appendUncompressed(params, uncompressed)
        self.appendChunkSize(params, chunk_size)
        self.appendLabels(params, labels)
        return self._execute(self.DECRBY_CMD,await self.redis.execute(self.DECRBY_CMD, *params))
        # return self.redis.execute(self.DECRBY_CMD, *params)

    async def createrule(self, source_key, dest_key, 
                     aggregation_type, bucket_size_msec):
        """
        Creates a compaction rule from values added to ``source_key``
        into ``dest_key``. Aggregating for ``bucket_size_msec`` where an
        ``aggregation_type`` can be ['avg', 'sum', 'min', 'max',
        'range', 'count', 'first', 'last', 'std.p', 'std.s', 'var.p', 'var.s']
        """
        params=[source_key, dest_key]
        self.appendAggregation(params, aggregation_type, bucket_size_msec)
        return self._execute(self.CREATERULE_CMD,await self.redis.execute(self.CREATERULE_CMD, *params))
        # return self.redis.execute(self.CREATERULE_CMD, *params)

    async def deleterule(self, source_key, dest_key):
        """Deletes a compaction rule"""
        return self._execute(self.DELETERULE_CMD,await self.redis.execute(self.DELETERULE_CMD, source_key, dest_key))
        # return self.redis.execute(self.DELETERULE_CMD, source_key, dest_key)

    async def __range_params(self, key, from_time, to_time, count, aggregation_type, bucket_size_msec):
        """
        Internal method to create TS.RANGE and TS.REVRANGE arguments
        """
        params = [key, from_time, to_time]
        self.appendCount(params, count)
        if aggregation_type is not None:
            self.appendAggregation(params, aggregation_type, bucket_size_msec)
        return params

    async def range(self, key, from_time, to_time, count=None,
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
        return self._execute(self.RANGE_CMD,await self.redis.execute(self.RANGE_CMD, *params))
        # return self.redis.execute(self.RANGE_CMD, *params)

    async def revrange(self, key, from_time, to_time, count=None,
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
        return self._execute(self.REVRANGE_CMD,await self.redis.execute(self.REVRANGE_CMD, *params))
        # return self.redis.execute(self.REVRANGE_CMD, *params)


    async def __mrange_params(self, aggregation_type, bucket_size_msec, count, filters, from_time, to_time, with_labels):
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

    async def mrange(self, from_time, to_time, filters, count=None,
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
        return self._execute(self.MRANGE_CMD,await self.redis.execute(self.MRANGE_CMD, *params))
        # return self.redis.execute(self.MRANGE_CMD, *params)

    async def mrevrange(self, from_time, to_time, filters, count=None,
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
        return self._execute(self.MREVRANGE_CMD,await self.redis.execute(self.MREVRANGE_CMD, *params))
        # return self.redis.execute(self.MREVRANGE_CMD, *params)

    async def get(self, key):
        """Gets the last sample of ``key``"""
        return self._execute(self.GET_CMD,await self.redis.execute(self.GET_CMD, key))
        # return self.redis.execute(self.GET_CMD, key)

    async def mget(self, filters, with_labels=False):
        """Get the last samples matching the specific ``filter``."""
        params = []
        self.appendWithLabels(params, with_labels)
        params.extend(['FILTER'])
        params += filters
        return self._execute(self.MGET_CMD,await self.redis.execute(self.MGET_CMD, *params))
        # return self.redis.execute(self.MGET_CMD, *params)
   
    async def info(self, key):
        """Gets information of ``key``"""
        return self._execute(self.INFO_CMD,await self.redis.execute(self.INFO_CMD, key))
        # return self.redis.execute(self.INFO_CMD, key)

    async def queryindex(self, filters):
        """Get all the keys matching the ``filter`` list."""
        return self._execute(self.QUERYINDEX_CMD,await self.redis.execute(self.QUERYINDEX_CMD, *filters))
        # return self.redis.execute(self.QUERYINDEX_CMD, *filters)

    async def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        return self.redis.pipeline()
        
