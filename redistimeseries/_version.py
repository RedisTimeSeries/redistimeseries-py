# This attribute is the only one place that the version number is written down,
# so there is only one place to change it when the version number changes.
# Edit the pyproject.toml to modify versions
def get_version():
    import importlib.metadata
    try:
        md = importlib.metadata.metadata('redistimeseries')
        return md['Version']
    except (IndexError, KeyError, importlib.metadata.PackageNotFoundError):
        return 'dev'

__version__ = get_version()
