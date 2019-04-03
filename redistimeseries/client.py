#import six
from redis import Redis,  RedisError 
'''Redis = StrictRedis from redis-py 3.0'''
from redis.client import Pipeline, parse_info, bool_ok
from redis.connection import Token
from redis._compat import (long, nativestr)
from redis.exceptions import DataError
#from .path import Path

def parse_range(response):
    pass

def parse_m_range(response):
    pass

class Client(Redis): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements ReJSON's
    commmands (prefixed with "json").
    The client allows to interact with RedisTimeSeries and use all of it's
    functionality 
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
            'TS.CREATE' : bool_ok,
            'TS.ADD': bool_ok,
            'TS.INCRBY': bool_ok,
            'TS.DECRBY': bool_ok,
            'TS.CREATERULE': bool_ok,
            'TS.DELETERULE':bool_ok,
            'TS.RANGE': parse_range,
            'TS.MRANGE': parse_m_range,
            #'TS.GET': , response is enough
            'TS.INFO': parse_info, # TODO
        }
        for k, v in six.iteritems(MODULE_CALLBACKS):
            self.set_response_callback(k, v)

    AGGREGATIONS = [None, 'avg', 'sum', 'min', 'max', 
                    'range', 'count', 'first', 'last']

    #Another idea
    class cls_AGGREGATIONS:
        NONE = None
        AVG = 'avg'
        SUM = 'sum'
        MIN = 'min'
        MAX = 'max'
        RANGE = 'range'
        COUNT = 'count'
        FIRST = 'first'
        LAST = 'last'
    
    def tsCreate(self, key, retention=None, *labels):
        '''Create a new time-series'''
        params = [key]
        if retention is not None:
            params.append(retention)
        if labels:
            params.append(labels)
        return self.execute_command('TS.CREATE', params)
        
    def tsAdd(self, key, timestamp, value, retention=None, *labels):
        '''Append (or create and append) a new value to the series'''
        params = [key, timestamp, value]
        if retention is not None:
            params.append(retention)
        params.append(labels)
        return self.execute_command('TS.ADD', params)

    def tsIncreaseBy(self, key, value, reset, retention=None, *labels): 
        '''Increment the latest value'''
        params = [key, value, reset]
        if retention is not None:
            params.append(retention)
        params.append(labels)
        return self.execute_command('TS.INCRBY', params)

    def tsDecreaseBy(self, key, value, reset, retention=None, *labels):  
        '''Decrease the latest value'''
        params = [key, value, reset]
        if retention is not None:
            params.append(retention)
        params.append(labels)
        return self.execute_command('TS.INCRBY', params)

    def tsCreateRule(self, sourceKey, destKey, 
                     aggregationType, bucketSizeSeconds):
        '''Create a compaction rule'''
        params = [sourceKey, destKey]
        if aggregationType not in self.AGGREGATIONS:
            raise DataError('Aggregation type is invalid')
        elif not isinstance(int(bucketSizeSeconds), int):
            raise DataError('bucketSizeSeconds is invalid')
        else:
            params.append(Token.get_token('AGGREGATION'))
            params.extend([aggregationType, bucketSizeSeconds])
        return self.execute_command('TS.CREATERULE', params)

    def tsDeleteRule(self, sourceKey, destKey):
        '''Delete a compaction rule'''
        return self.execute_command('TS.DELETERULE', sourceKey, destKey)
   
    def tsRange(self, key, fromTime, toTime, 
                aggregationType = None, bucketSizeSeconds = 0):
        '''Query a range'''
        params = [key, fromTime, toTime]
        if aggregationType is not None:        
            if aggregationType not in self.AGGREGATIONS:
                raise DataError('Aggregation type is invalid')
            elif not isinstance(int(bucketSizeSeconds), int):
                raise DataError('bucketSizeSeconds is invalid')
            else:
                params.append(Token.get_token('AGGREGATION'))
                params.extend([aggregationType, bucketSizeSeconds])
        return self.execute_command('TS.RANGE', params)

    def tsMultiRange(self, fromTime, toTime, *filters, 
                     aggregationType = None, bucketSizeSeconds = 0):
        '''Query a range by filters'''
        params = [fromTime, toTime]
        if aggregationType is not None:        
            if aggregationType not in self.AGGREGATIONS:
                raise DataError('Aggregation type is invalid')
            elif not isinstance(int(bucketSizeSeconds), int):
                raise DataError('bucketSizeSeconds is invalid')
            else:
                params.append(Token.get_token('AGGREGATION'))
                params.extend([aggregationType, bucketSizeSeconds])
        params.append(*filters)
        return self.execute_command('TS.MRANGE', params)

    def tsGet(self, key):
        '''Get the last sample'''
        return self.execute_command('TS.GET', key)

    def tsInfo(self, key):
        '''Get the last sample'''
        return self.execute_command('TS.INFO', key)

client = Client()
client.tsCreate(1)