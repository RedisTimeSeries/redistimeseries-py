from ._version import __version__
from warnings import warn
warn("Please upgrade to redis-py (https://pypi.org/project/redis/) "
"This library is deprecated, and all features have been merged into redis-py.",
DeprecationWarning, stacklevel=2)
