package org.yonigibbs.crdt

import java.time.Clock
import java.time.Instant
import java.util.*

// TODO: think about thread-safety/concurrent access?
// TODO: make whole thing immutable?
// TODO: implement Map interface?
// TODO: add constructor that takes in a predefined state? Would involve exposing the inner classes.
// TODO: implement equals, hashCode, toString, etc

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

    private constructor(
        upserts: Map<Key, Upsert<Value, Timestamp, PeerId>>,
        removals: Map<Key, Removal<Timestamp, PeerId>>,
        peerId: PeerId,
        getCurrentTimestamp: () -> Timestamp
    ) : this(peerId, getCurrentTimestamp) {
        upserts.forEach { (key, value) -> this.upserts[key] = value }
        removals.forEach { (key, value) -> this.removals[key] = value }
    }

    operator fun get(key: Key): Value? {
        // If the addition element isn't present then early return null: doesn't matter if it's in the removals set or
        // not.
        val upsert = upserts[key] ?: return null

        // If it's not in the removals set then we can just return the one from the additions set.
        val removal = removals[key] ?: return upsert.value

        // If we get here we have it in both the addition and removal set: compare timestamps (and, if necessary, peer
        // IDs).
        return upsert.value.takeIf { upsert.supersedes(removal) }
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

    // TODO: every time this is called it's re-evaluated. Consider or document option of having this kept updates on
    //  every upsert/removal.
    val entries: Map<Key, Value>
        get() = upserts
            .filter { (key, upsert) ->
                val removal = removals[key]
                removal == null || !removal.supersedes(upsert)
            }.map { (key, upsert) ->
                key to upsert.value
            }.toMap()

    private fun <A : Action<Timestamp, PeerId>> MutableMap<Key, A>.addAllSupersedingActions(actions: Map<Key, A>) {
        actions
            .filter { (key, newAction) ->
                val existingAction = this[key]
                existingAction == null || newAction.supersedes(existingAction)
            }.forEach { (key, newAction) ->
                this[key] = newAction
            }
    }

    fun merge(other: LwwElementDictionary<Key, Value, Timestamp, PeerId>) {
        upserts.addAllSupersedingActions(other.upserts)
        removals.addAllSupersedingActions(other.removals)
    }

    private fun Action<Timestamp, PeerId>.supersedes(other: Action<Timestamp, PeerId>) =
        when {
            this.timestamp > other.timestamp -> true
            this.timestamp < other.timestamp -> false
            else -> this.peerId >= other.peerId
        }

    fun clone() = LwwElementDictionary(upserts, removals, peerId, getCurrentTimestamp)
}

// TODO: leave here, or just add as comment to explain?
class DefaultLwwElementDictionary<Key, Value>(
    peerId: UUID,
    private val clock: Clock = Clock.systemUTC()
) : LwwElementDictionary<Key, Value, Instant, UUID>(
    peerId = peerId,
    getCurrentTimestamp = { Instant.now(clock) }
)