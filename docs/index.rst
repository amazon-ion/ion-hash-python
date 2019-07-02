.. ion-hash-python documentation master file, created by
   sphinx-quickstart on Thu Jun 27 08:39:32 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ion-hash-python
===============
.. toctree::
   :maxdepth: 1

API documentation for the `ion-hash-python <https://github.com/amzn/ion-hash-python>`_ GitHub repository.

ionhash.hasher module
---------------------
.. automodule:: ionhash.hasher
   :members:
   :undoc-members:
   :show-inheritance:

.. method:: <simpleion_class>.ion_hash(algorithm=None, hash_function_provider=None)

   Given an algorithm or hash_function_provider, computes the Ion hash
   of this value.

   Args:
       algorithm:
           A string corresponding to the name of a hash algorithm supported
           by the `hashlib` module.

       hash_function_provider:
           A function that returns a new ``IonHasher`` instance when called.

           Note that multiple ``IonHasher`` instances may be required to hash a single value
           (depending on the type of the Ion value).

   Returns:
       `bytes` that represent the Ion hash of this value for the specified algorithm
       or hash_function_provider.


.. autofunction:: ionhash.hasher.hash_reader(reader, hash_function_provider)
.. autofunction:: ionhash.hasher.hash_writer(writer, hash_function_provider)

