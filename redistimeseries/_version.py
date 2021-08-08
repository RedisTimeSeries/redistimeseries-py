# This attribute is the only one place that the version number is written down,
# so there is only one place to change it when the version number changes.
# Edit the pyproject.toml to modify versions
def get_version():
    try:
        from importlib.metadata import version
    except ModuleNotFoundError:  # python 3.6, 3.7
        from importlib_metadata import version

    try:
        md = importlib.metadata.metadata('redistimeseries')
        return version('redistimeseries')
    except:
        return 'dev'


__version__ = get_version()
