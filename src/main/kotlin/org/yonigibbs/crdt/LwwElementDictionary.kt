package org.yonigibbs.crdt

import java.time.Clock
import java.time.Instant

// TODO: think about thread-safety/concurrent access?
// TODO: make whole thing immutable?

open class LwwElementDictionary<Key, Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
    private val peerId: PeerId,
    private val getCurrentTimestamp: () -> Timestamp
) {
    private data class Element<Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
        val value: Value,
        val timestamp: Timestamp,
        val peerId: PeerId
    )

    private val additions = mutableMapOf<Key, Element<Value, Timestamp, PeerId>>()
    private val removals = mutableMapOf<Key, Element<Value, Timestamp, PeerId>>()

    operator fun get(key: Key): Value? {
        // If the addition element isn't present then early return null: doesn't matter if it's in the removals set or
        // not.
        val additionElement = additions[key] ?: return null

        // If it's not in the removals set then we can just return the one from the additions set.
        val removalElement = removals[key] ?: return additionElement.value

        // If we get here we have it in both the addition and removal set: compare timestamps. If timestamps are equal,
        // prefer the addition.
        return additionElement.value.takeIf { removalElement.timestamp <= additionElement.timestamp }
    }

    operator fun set(key: Key, value: Value) {
        val existingAddition = additions[key]
        val newAddition = Element(value, getCurrentTimestamp(), peerId)

        if (existingAddition == null || newAddition.supersedes(existingAddition))
            additions[key] = newAddition
    }

//    operator fun minusAssign(key: Key) {
//        val existingRemoval = removals[key]
//        val newRemoval = Element(value, getCurrentTimestamp(), peerId)
//
//        if (existingAddition == null || newAddition.supersedes(existingAddition))
//            additions[key] = newAddition
//    }

    private fun Element<Value, Timestamp, PeerId>.supersedes(other: Element<Value, Timestamp, PeerId>) =
        when {
            this.timestamp > other.timestamp -> true
            this.timestamp < other.timestamp -> false
            else -> this.peerId >= other.peerId
        }
}

class SystemTimeLwwElementDictionary<Key, Value, PeerId : Comparable<PeerId>>(
    peerId: PeerId,
    private val clock: Clock = Clock.systemUTC()
) : LwwElementDictionary<Key, Value, Instant, PeerId>(
    peerId = peerId,
    getCurrentTimestamp = { Instant.now(clock) }
)
