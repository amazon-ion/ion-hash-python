from setuptools import setup, find_packages

setup(
    name='ionhash',
    version='1.3.0-SNAPSHOT',
    description='Python implementation of Amazon Ion Hash',
    url='http://github.com/amzn/ion-hash-python',
    author='Amazon Ion Team',
    author_email='ion-team@amazon.com',
    license='Apache License 2.0',

    packages=find_packages(exclude=['tests*']),

    setup_requires=[
        'pytest-runner',
    ],

    tests_require=[
        'pytest',
    ],
)
