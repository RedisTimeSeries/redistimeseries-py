from setuptools import setup, find_packages
import io

def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()

def read_version(version_file):
    """
    Given the input version_file, this function extracts the
    version info from the __version__ attribute.
    """
    version_str = None
    import re
    verstrline = open(version_file, "rt").read()
    VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        version_str = mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (version_file,))
    return version_str


requirements = list(map(str.strip, open("requirements.txt").readlines()))

setup(
    name='redistimeseries',
    version=read_version("redistimeseries/_version.py"),
    description='RedisTimeSeries Python Client',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='https://github.com/RedisTimeSeries/redistimeseries-py',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ],
    keywords='Redis TimeSeries Extension',
    author='RedisLabs',
    author_email='oss@redislabs.com'
)
