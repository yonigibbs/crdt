package org.yonigibbs.crdt

import java.time.Clock
import java.time.Instant
import java.util.*

// TODO: think about thread-safety/concurrent access?
// TODO: make whole thing immutable?
// TODO: implement Map interface?
// TODO: ensure code coverage 100%?

/**
 * A rudimentary implementation of an LWW element dictionary (aka map). Uses last-write-wins semantics to handle
 * conflicts when merging together two instances of the CRDT together. See [merge] for more details.
 *
 * This is expected to be maintained locally by a single peer (in a network of peers collaborating on the CRDT), with
 * functions such as [set] and [remove]. Other copies of the CRDT from other peers are merged in using [merge].
 *
 * @param peerId The ID of the peer which is locally maintaining this copy of the CRDT.
 * @param getCurrentTimestamp The function used to get the current timestamp of actions as they are called on this class
 * (e.g. [set] or [remove]). Typically the type, [Timestamp], would be [Instant], and `getCurrentTimestamp` would simply
 * return `Instant.now()`.
 *
 * @param Key The type of the key in the dictionary.
 * @param Value The type of the value in the dictionary. Can be nullable if required.
 * @param Timestamp The type representing the timestamp of any update to the dictionary. Typically [Instant] can be
 * used, e.g. with the system clock. However, there's no guarantee that any two peers that share data have the same
 * system time. Therefore, this generic type parameter is used to allow other timestamp implementations to be used, e.g.
 * [lamport clocks](https://adamwulf.me/2021/05/distributed-clocks-and-crdts/#lamport-clocks). The only stipulation on
 * this type is that it must implement [Comparable] to allow the code to choose which event occurred first. In tests,
 * [Int] can be used for simplicity.
 * @param PeerId The type that represents an individual peer interacting with the CRDT. Typically [UUID] or [String] can
 * be used. Again, the only stipulation is that the type must implement [Comparable]: this is because if two events have
 * the same timestamp then the peer ID can be used to choose a "winner" in a consistent way, ensuring that ever peer
 * makes the same choice, thereby keeping the CRDTs consistent.
 */
open class LwwElementDictionary<Key, Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
    private val peerId: PeerId,
    private val getCurrentTimestamp: () -> Timestamp
) {
    /**
     * An action that occurred on this CRDT, namely an [Upsert] (update or insert) or a [Removal].
     */
    private interface Action<Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>> {
        val timestamp: Timestamp
        val peerId: PeerId
    }

    /**
     * An action which either inserts a value in the dictionary, or updates the value associated with a pre-existing key.
     */
    private data class Upsert<Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
        val value: Value,
        override val timestamp: Timestamp,
        override val peerId: PeerId
    ) : Action<Timestamp, PeerId>

    /**
     * An action which removes an entry from the dictionary.
     */
    private data class Removal<Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>(
        override val timestamp: Timestamp,
        override val peerId: PeerId
    ) : Action<Timestamp, PeerId>

    private val upserts = mutableMapOf<Key, Upsert<Value, Timestamp, PeerId>>()
    private val removals = mutableMapOf<Key, Removal<Timestamp, PeerId>>()

    /**
     * A private constructor used when cloning the dictionary.
     */
    private constructor(
        upserts: Map<Key, Upsert<Value, Timestamp, PeerId>>,
        removals: Map<Key, Removal<Timestamp, PeerId>>,
        peerId: PeerId,
        getCurrentTimestamp: () -> Timestamp
    ) : this(peerId, getCurrentTimestamp) {
        this.upserts.putAll(upserts)
        this.removals.putAll(removals)
    }

    /**
     * Gets the value associated with the given `key`, or `null` if the dictionary doesn't contain an entry with this
     * key. Note that if the value stored against the given key is explicitly null, this function does not allow for a
     * way of disambiguating between a value being missing and value having null stored against it: this is also true of
     * [Map] itself. For this, use [containsKey].
     */
    operator fun get(key: Key): Value? = getActiveUpsertForKey(key)?.value

    /**
     * Gets a boolean indicating whether an entry with the given key exists in the dictionary.
     */
    fun containsKey(key: Key): Boolean = getActiveUpsertForKey(key) != null

    /**
     * Gets the [Upsert] related to the given `key`, if it exists and isn't superseded by a [Removal] (i.e. it is
     * considered "active"). If not active or not present at all, null is returned.
     */
    private fun getActiveUpsertForKey(key: Key): Upsert<Value, Timestamp, PeerId>? {
        // If the upsert isn't present then early return null: doesn't matter if it's in the removals set or not.
        val upsert = upserts[key] ?: return null

        // If it's not in the removals set then we can just return the upsert.
        val removal = removals[key] ?: return upsert

        // If we get here we have it in both the addition and removal set: compare timestamps (and, if necessary, peer
        // IDs) to decide the winner.
        return upsert.takeIf { it.supersedes(removal) }
    }

    /**
     * Sets the given `value` against the given `key`, unless there's a more recent upsert already in the set. This
     * should never happen if the CRDT is only updated by a single peer (i.e. updates from other peers are all handled
     * in [merge]).
     */
    operator fun set(key: Key, value: Value) {
        val existingAddition = upserts[key]
        val newAddition = Upsert(value, getCurrentTimestamp(), peerId)

        if (existingAddition == null || newAddition.supersedes(existingAddition))
            upserts[key] = newAddition
    }

    /**
     * Removes the entry associated with the given `key`.
     */
    fun remove(key: Key) {
        val existingRemoval = removals[key]
        val newRemoval = Removal(getCurrentTimestamp(), peerId)

        if (existingRemoval == null || newRemoval.supersedes(existingRemoval))
            removals[key] = newRemoval
    }

    /**
     * Removes the entry associated with the given `key`.
     */
    operator fun minusAssign(key: Key) = remove(key)

    /**
     * Gets all the entries in the dictionary, as a [Map].
     */
    // Note: every time this is called it's re-evaluated. This is potentially inefficient. An alternative, especially in
    // a read-heavy use case, would be to maintain a map which is updated every time `set` or `remove` are called, and
    // return that from this call. This would take more memory (as there's a second map stored in memory), but this call
    // would be faster.
    val entries: Map<Key, Value>
        get() = upserts
            .filter { (key, upsert) ->
                val removal = removals[key]
                removal == null || !removal.supersedes(upsert)
            }.map { (key, upsert) ->
                key to upsert.value
            }.toMap()

    /**
     * Merges the data from `other` into `this`. Uses the [Action.timestamp] value on each action to decide the winner
     * (the last write wins). In the case where two actions have the same timestamp, the [Action.peerId] is used to
     * decide the winner. This is arbitrary, but ensures consistency.
     */
    fun merge(other: LwwElementDictionary<Key, Value, Timestamp, PeerId>) {
        upserts.addAllSupersedingActions(other.upserts)
        removals.addAllSupersedingActions(other.removals)
    }

    /**
     * Adds all items from `actions` into `this` [MutableMap], unless those actions are superseded by pre-existing
     * items in `this`.
     */
    private fun <A : Action<Timestamp, PeerId>> MutableMap<Key, A>.addAllSupersedingActions(actions: Map<Key, A>) {
        actions
            .filter { (key, newAction) ->
                val existingAction = this[key]
                existingAction == null || newAction.supersedes(existingAction)
            }.forEach { (key, newAction) ->
                this[key] = newAction
            }
    }

    /**
     * Gets a boolean indicating whether `this` [Action] supersedes `action` (i.e. is "the winner"). Uses the
     * [Action.timestamp] value, and in the case of two actions having the same timestamp, falls back to using the
     * [Action.peerId]. This is arbitrary, but ensures consistency.
     */
    private fun Action<Timestamp, PeerId>.supersedes(other: Action<Timestamp, PeerId>) =
        when {
            this.timestamp > other.timestamp -> true
            this.timestamp < other.timestamp -> false
            else -> this.peerId >= other.peerId
        }

    /**
     * Clones this dictionary.
     */
    fun clone() = LwwElementDictionary(upserts, removals, peerId, getCurrentTimestamp)

    /**
     * Returns a boolean indicating whether `this` has the same contents as `other`. Note that this is different from
     * [equals] because here the value of this class's [peerId] property is ignored, whereas in [equals] it's not. In
     * other words, two copies of the CRDT which have been replicated across two peers will return `true` for
     * [contentEquals] but `false` for [equals].
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
     * [contentEquals] because here the value of this class's [peerId] property is respected, whereas in [equals] it's
     * ignored. In other words, two copies of the CRDT which have been replicated across two peers will return `true`
     * for [contentEquals] but `false` for [equals].
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

    // This is potentially dangerous: CRDTs monotonically increase in size (calling `remove`) doesn't actually make
    // the size any smaller. So including every upsert and removal in the `toString` implementation could return a very
    // long string. However other built-in classes in the standard library (e.g. Map itself) return all entries in their
    // implementation of `toString`, and they could all have large amounts of data in them too, so maybe it's OK. At
    // least it's consistent with the classes from the standard library.
    override fun toString() = "LwwElementDictionary(peerId=$peerId, upserts=$upserts, removals=$removals)"
}

/**
 * The default concrete implementation of [LwwElementDictionary] with the default set of generic type parameters.
 * Namely, the timestamp type is [Instant] and the peer ID is a [UUID]. This is probably the default option for real
 * uses of the dictionary; in tests, however, we use types that are easier to work with (integers for the timestamp;
 * strings for the peer ID).
 *
 * Normally `clock` would be left with the default value, but if required this can be overridden (e.g. in tests
 * `Clock.fixed` can be supplied with some predefined hard-coded value).
 */
class DefaultLwwElementDictionary<Key, Value>(
    peerId: UUID,
    private val clock: Clock = Clock.systemUTC()
) : LwwElementDictionary<Key, Value, Instant, UUID>(
    peerId = peerId,
    getCurrentTimestamp = { Instant.now(clock) }
)