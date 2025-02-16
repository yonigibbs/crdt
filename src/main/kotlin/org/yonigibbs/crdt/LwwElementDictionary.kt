package org.yonigibbs.crdt

import java.time.Clock
import java.time.Instant
import java.util.*

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

    private constructor(
        upserts: Map<Key, Upsert<Value, Timestamp, PeerId>>,
        removals: Map<Key, Removal<Timestamp, PeerId>>,
        peerId: PeerId,
        getCurrentTimestamp: () -> Timestamp
    ) : this(peerId, getCurrentTimestamp) {
        upserts.forEach { (key, value) -> this.upserts[key] = value }
        removals.forEach { (key, value) -> this.removals[key] = value }
    }

    /**
     * Gets the value associated with the given `key`, or `null` if the dictionary doesn't contain an entry with this
     * key. Note that if the value stored against the given key is explicitly null, this function does not allow for a
     * way of disambiguating between a value being missing and value having null stored against it: this is also true of
     * [Map] itself. For this, use [containsKey].
     */
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

    fun containsKey(key: Key): Boolean {
        val upsert = upserts[key] ?: return false
        val removal = removals[key] ?: return true
        return upsert.supersedes(removal)
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

    /**
     * Returns a boolean indicating whether `this` has the same contents as `other`. Note that this is different from
     * [equals] because here the value of [peerId] is ignored, whereas in [equals] it's not.
     */
    fun contentEquals(other: LwwElementDictionary<Key, Value, Timestamp, PeerId>): Boolean {
        if (other.upserts.size != this.upserts.size) return false
        if (other.removals.size != this.removals.size) return false

        for ((key, thisUpsert) in this.upserts) {
            val otherUpsert = other.upserts[key] ?: return false
            if (thisUpsert != otherUpsert) return false
        }

        for ((key, thisRemoval) in this.removals) {
            val otherRemoval = other.removals[key] ?: return false
            if (thisRemoval != otherRemoval) return false
        }

        return true
    }

    /**
     * Returns a boolean indicating whether `this` is exactly equal to `other`. Note that this is different from
     * [contentEquals] because here the value of [peerId] is respected, whereas in [equals] it's ignored.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LwwElementDictionary<*, *, *, *>) return false

        if (peerId != other.peerId) return false
        if (upserts != other.upserts) return false
        if (removals != other.removals) return false

        return true
    }

    override fun hashCode(): Int {
        var result = peerId.hashCode()
        result = 31 * result + upserts.hashCode()
        result = 31 * result + removals.hashCode()
        return result
    }

    override fun toString() = "LwwElementDictionary(peerId=$peerId, upserts=$upserts, removals=$removals)"
}

class DefaultLwwElementDictionary<Key, Value>(
    peerId: UUID,
    private val clock: Clock = Clock.systemUTC()
) : LwwElementDictionary<Key, Value, Instant, UUID>(
    peerId = peerId,
    getCurrentTimestamp = { Instant.now(clock) }
)