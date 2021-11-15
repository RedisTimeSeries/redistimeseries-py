[![license](https://img.shields.io/github/license/RedisTimeSeries/redistimeseries-py.svg)](https://github.com/RedisTimeSeries/redistimeseries-py)
[![PyPI version](https://badge.fury.io/py/redistimeseries.svg)](https://badge.fury.io/py/redistimeseries)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/redistimeseries-py/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/redistimeseries-py/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/redistimeseries-py.svg)](https://github.com/RedisTimeSeries/redistimeseries-py/releases/latest)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/redistimeseries-py/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/redistimeseries-py)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/RedisTimeSeries/redistimeseries-py.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisTimeSeries/redistimeseries-py/context:python)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisTimeSeries/redistimeseries-py/badge.svg?targetFile=pyproject.toml)](https://snyk.io/test/github/RedisTimeSeries/redistimeseries-py?targetFile=pyproject.toml)

# redistimeseries-py
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

## Deprecation notice

As of [redis-py 4.0.0](https://pypi.org/project/redis/) this library is deprecated. It's features have been merged into redis-py. Please either install it [from pypy](https://pypi.org/project/redis) or [the repo](https://github.com/redis/redis-py).

--------------------------------

redistimeseries-py is a package that gives developers easy access to RedisTimeSeries module. The package extends [redis-py](https://github.com/andymccurdy/redis-py)'s interface with RedisTimeSeries's API.

## Installation
```
$ pip install redistimeseries
```

## Development

1. Create a virtualenv to manage your python dependencies, and ensure it's active.
   ```virtualenv -v venv```
2. Install [pypoetry](https://python-poetry.org/) to manage your dependencies.
   ```pip install poetry```
3. Install dependencies.
   ```poetry install```

[tox](https://tox.readthedocs.io/en/latest/) runs all tests as its default target. Running *tox* by itself will run unit tests. Ensure you have a running redis, with the module loaded.



## API
The complete documentation of RedisTimeSeries's commands can be found at [RedisTimeSeries's website](http://redistimeseries.io/).

## Usage example

```python
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

## Further notes on back-filling time series

Since [RedisTimeSeries 1.4](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/tag/v1.4.5) we've added the ability to back-fill time series, with different duplicate policies.

The default behavior is to block updates to the same timestamp, and you can control it via the `duplicate_policy` argument. You can check in detail the [duplicate policy documentation](https://oss.redislabs.com/redistimeseries/configuration/#duplicate_policy).

Bellow you can find an example of the `LAST` duplicate policy, in which we override duplicate timestamps with the latest value:

```python
from redistimeseries.client import Client
rts = Client()
rts.create('last-upsert', labels={'Time':'Series'}, duplicate_policy='last')
rts.add('last-upsert', 1, 10.0)
rts.add('last-upsert', 1, 5.0)
# should output [(1, 5.0)]
print(rts.range('last-upsert', 0, -1))
```

## License
[BSD 3-Clause](https://github.com/ashtul/redistimeseries-py/blob/master/LICENSE)
