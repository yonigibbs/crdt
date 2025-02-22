# Overview

This repository contains a rudimentary implementation of a last-write-wins element dictionary.

The implementation is in [LwwElementDictionary.kt](src/main/kotlin/org/yonigibbs/crdt/LwwElementDictionary.kt). The
class takes some generic type parameters, as explained in the KDocs on the class itself.

The main public functions are:

* `set`: sets an entry in the dictionary (can be used to add a new entry or update an existing entry, aka "upsert").
* `get`: gets the value in the dictionary for the given key, or null if there's no such entry.
* `remove`: removes the entry with the given key.
* `merge`: merges one dictionary into another. This function is commutative, associative, and idempotent, and there are
  unit tests for each of these properties.

All the functions have KDocs which provide a bit more detail.

The tests are in [LwwElementDictionaryTest.kt](src/test/kotlin/org/yonigibbs/crdt/LwwElementDictionaryTest.kt). Code
coverage is 100%.

# Exclusions / possible enhancements

* The class is not thread-safe: if two threads concurrently update an instance, unexpected behaviour could result. A
  thread-safe version could be created, using synchronisation or locking to protect against this.
* The dictionary acts as a mutable data structure, just like a `MutableMap`. An alternative approach would be to have it
  all immutable, such that any action which mutates the data returns a new instance of the dictionary, with the
  mutations applied, rather than mutating the dictionary's internal state directly.
* The dictionary could be enhanced to implement the `MutableMap` interface.

