package org.yonigibbs.crdt

import java.time.Clock
import java.time.Instant

// TODO: think about thread-safety/concurrent access?
// TODO: make whole thing immutable?
// TODO: implement Map interface?

open class LwwElementDictionary<Key, Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
    private val peerId: PeerId,
    private val getCurrentTimestamp: () -> Timestamp
) {
    private interface Action<Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>> {
        val timestamp: Timestamp
        val peerId: PeerId
    }

    private data class Upsert<Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
        val value: Value,
        override val timestamp: Timestamp,
        override val peerId: PeerId
    ) : Action<Timestamp, PeerId>

    private data class Removal<Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
        override val timestamp: Timestamp,
        override val peerId: PeerId
    ) : Action<Timestamp, PeerId>

    private val upserts = mutableMapOf<Key, Upsert<Value, Timestamp, PeerId>>()
    private val removals = mutableMapOf<Key, Removal<Timestamp, PeerId>>()

    operator fun get(key: Key): Value? {
        // If the addition element isn't present then early return null: doesn't matter if it's in the removals set or
        // not.
        val additionElement = upserts[key] ?: return null

        // If it's not in the removals set then we can just return the one from the additions set.
        val removalElement = removals[key] ?: return additionElement.value

        // If we get here we have it in both the addition and removal set: compare timestamps. If timestamps are equal,
        // prefer the addition.
        return additionElement.value.takeIf { removalElement.timestamp <= additionElement.timestamp }
    }

    operator fun set(key: Key, value: Value) {
        val existingAddition = upserts[key]
        val newAddition = Upsert(value, getCurrentTimestamp(), peerId)

        if (existingAddition == null || newAddition.supersedes(existingAddition))
            upserts[key] = newAddition
    }

    fun remove(key: Key) {
        val existingRemoval = removals[key]
        val newRemoval = Removal(getCurrentTimestamp(), peerId)

        if (existingRemoval == null || newRemoval.supersedes(existingRemoval))
            removals[key] = newRemoval
    }

    operator fun minusAssign(key: Key) = remove(key)

    private fun Action<Timestamp, PeerId>.supersedes(other: Action<Timestamp, PeerId>) =
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
