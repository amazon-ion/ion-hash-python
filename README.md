# Amazon Ion Hash Python
An implementation of [Amazon Ion Hash](http://amzn.github.io/ion-hash) in Python.

[![Build Status](https://travis-ci.org/amzn/ion-hash-python.svg?branch=master)](https://travis-ci.org/amzn/ion-hash-python)
[![Documentation Status](https://readthedocs.org/projects/ion-hash-python/badge/?version=latest)](https://ion-hash-python.readthedocs.io/en/latest/?badge=latest)

This package is designed to work with **Python 3.4+**.

## Getting Started

Computing the Ion hash of a simpleion value may be done by calling the `ion_hash()` method.  For example:

```
>>> import amazon.ion.simpleion as ion
>>> import ionhash
>>> obj = ion.loads('[1, 2, 3]')
>>> digest = obj.ion_hash('md5')
>>> print('digest:', ''.join(' %02x' % x for x in digest))
digest:  8f 3b f4 b1 93 5c f4 69 c9 c1 0c 31 52 4b 26 25
```

Alternatively, lower-level hash_reader/hash_writer APIs may be used to compute an Ion hash:

```python
from io import BytesIO

from amazon.ion.core import IonEventType
from amazon.ion.reader import blocking_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader

from ionhash.hasher import hash_reader
from ionhash.hasher import hashlib_hash_function_provider
from ionhash.hasher import HashEvent


ion = b'[1, 2, 3]'
hash_function = "md5"

reader = hash_reader(
    blocking_reader(managed_reader(text_reader(), None), BytesIO(ion)),
    hashlib_hash_function_provider(hash_function))

while True:
    event = reader.send(NEXT_EVENT)
    if event.event_type == IonEventType.STREAM_END:
        break

digest = reader.send(HashEvent.DIGEST)
print('digest:', ''.join(' %02x' % x for x in digest))
```

When run, it produces the following output:
```
digest:  8f 3b f4 b1 93 5c f4 69 c9 c1 0c 31 52 4b 26 25
```

## Development

This repository contains a git submodule called `ion-hash-test` which holds data used by `ion-hash-python`'s unit tests.

The easiest way to clone the ion-hash-python repository and initialize its `ion-hash-test` submodule is to run the 
following command:

```
$ git clone --recursive https://github.com/amzn/ion-hash-python.git ion-hash-python
```

Alternatively, the submodule may be initialized independently from the clone by running the following commands:

$ git submodule init
$ git submodule update

Once the repository has been fully initialized, `cd` into it:

```
$ cd ion-hash-python
```

Then use `venv` to create a clean environment to build/test `ion-hash-python` as follows:

```
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ pip install -e .
```

You should then be able to run the test suite by executing `python setup.py test`, or simply running `py.test`.

### Tox Setup
In order to verify that ion-hash-python works on all supported platforms, we use a combination
of [tox](http://tox.readthedocs.io/en/latest/) with [pyenv](https://github.com/yyuu/pyenv).

Install relevant versions of Python:
```
$ for V in 3.4.10 3.5.7 3.6.8 3.7.3 pypy3.6-7.1.0; do pyenv install $V; done
```

Once you have these installations, add each as a local `pyenv` configuration:
```
$ pyenv local 3.4.10 3.5.7 3.6.8 3.7.3 pypy3.6-7.1.0
```

Assuming you have `pyenv` properly set up (making sure `pyenv init` is evaluated into your shell),
you can now run `tox`:

```
# Run tox for all versions of Python (this executes `py.test` for each supported platform):
$ tox

# Run tox for just Python 3.4 and 3.5:
$ tox -e py34,py35

# Run tox for a specific version and run py.test with high verbosity:
$ tox -e py34 -- py.test -vv

# Run tox for a specific version and start the virtual env's Python REPL:
$ tox -e py35 -- python
```

## License

This library is licensed under the Apache 2.0 License. 

