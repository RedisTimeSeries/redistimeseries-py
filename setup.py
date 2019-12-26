from setuptools import setup, find_packages
import io

def read_all(f):
    with io.open(f, encoding="utf-8") as I:
        return I.read()

requirements = map(str.strip, open("requirements.txt").readlines())

setup(
    name='redistimeseries',
    version='0.6.1',
    description='RedisTimeSeries Python Client',
    long_description=read_all("README.md"),
    long_description_content_type='text/markdown',
    url='https://github.com/RedisTimeSeries/redistimeseries-py',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ],
    keywords='Redis TimeSeries Extension',
    author='RedisLabs',
    author_email='oss@redislabs.com'
)
