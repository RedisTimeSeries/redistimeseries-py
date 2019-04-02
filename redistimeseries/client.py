import six
import json
from redis import Redis,  RedisError, ConnectionPool 
'''Redis = StrictRedis from redis-py 3.0'''
from redis.client import Pipeline, parse_info, bool_ok
from redis._compat import (long, nativestr)
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

    def __init__(self, encoder=None, decoder=None, *args, **kwargs):
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
            
