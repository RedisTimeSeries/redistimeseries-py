from setuptools import setup, find_packages
setup(
    name='redistimeseries',
    version='0.1',

    description='RedisTimeSeries Python Client',
    url='https://github.com/RedisTimeSeries/redistimeseries-py',
    packages=find_packages(),
    install_requires=['redis'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
    ]
)
