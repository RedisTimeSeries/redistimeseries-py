[![license](https://img.shields.io/github/license/RedisTimeSeries/redistimeseries-py.svg)](https://github.com/RedisTimeSeries/redistimeseries-py)
[![PyPI version](https://badge.fury.io/py/redistimeseries.svg)](https://badge.fury.io/py/redistimeseries)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/redistimeseries-py/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/redistimeseries-py/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/redistimeseries-py.svg)](https://github.com/RedisTimeSeries/redistimeseries-py/releases/latest)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/redistimeseries-py/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/redistimeseries-py)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/RedisTimeSeries/redistimeseries-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/redistimeseries-py/context:python)

# redistimeseries-py
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisTimeSeries.svg)](https://gitter.im/RedisLabs/RedisTimeSeries?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

redistimeseries-py is a package that gives developers easy access to RedisTimeSeries module. The package extends [redis-py](https://github.com/andymccurdy/redis-py)'s interface with RedisTimeSeries's API.  

## Installation
``` 
$ pip install redistimeseries
```

## API
The complete documentation of RedisTimeSeries's commands can be found at [RedisTimeSeries's website](http://redistimeseries.io/).

## Usage example

```sql
# Simple example
from redistimeseries.client import Client
rts = Client()
rts.create('test', labels={'Time':'Series'})
rts.add('test', 1, 1.12)
rts.add('test', 2, 1.12)
rts.get('test')                                  
rts.incrby('test',1)                               
rts.range('test', 0, -1)
rts.range('test', 0, -1, aggregation_type='avg', bucket_size_msec=10)
rts.range('test', 0, -1, aggregation_type='sum', bucket_size_msec=10)
rts.info('test').__dict__

# Example with rules
rts.create('source', retention_msecs=40)
rts.create('sumRule')
rts.create('avgRule')
rts.createrule('source', 'sumRule', 'sum', 20)
rts.createrule('source', 'avgRule', 'avg', 15)
rts.add('source', '*', 1)
rts.add('source', '*', 2)
rts.add('source', '*', 3)
rts.get('sumRule')
rts.get('avgRule')
rts.info('sumRule').__dict__
```

## License
[BSD 3-Clause](https://github.com/ashtul/redistimeseries-py/blob/master/LICENSE)
