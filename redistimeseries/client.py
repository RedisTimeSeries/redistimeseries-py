import six
import json
from redis import Redis,  RedisError, ConnectionPool 
'''Redis = StrictRedis from redis-py 3.0'''
from redis.client import Pipeline
from redis._compat import (long, nativestr)
#from .path import Path

