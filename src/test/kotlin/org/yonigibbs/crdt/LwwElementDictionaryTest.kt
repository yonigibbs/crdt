package org.yonigibbs.crdt

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.*

/**
 * The type of the dictionary used in most of the tests. Uses [Int] as the timestamp type, and [String] as the peer ID.
 * This allows the tests to work with these simpler types, keeping the test code simpler and easier to work with.
 *
 * The key and value in the dictionary, for simplicity, are both strings.
 */
private typealias Dictionary = LwwElementDictionary<String, String?, Int, String>

/**
 * Tests for the [LwwElementDictionary].
 */
class LwwElementDictionaryTest {
    /**
     * The timestamp to set against the next action (upsert/removal) on the dictionary. Typically incremented after
     * every action, though some tests avoid this, to test conflict resolution. This is reset back to 0 before each test
     * runs.
     */
    var timestamp = 0

    private fun getNewDictionary(peerId: String = "peerA") = Dictionary(peerId) { timestamp }

    /**
     * Sets the given [value] against the given [key] in `this` dictionary. If [incrementTimestamp] is true (the default)
     * then [timestamp] is incremented after the action.
     *
     * This is just a helper function to avoid doing this everywhere:
     * ```
     * this[key] = value
     * timestamp++  // Ensures we don't forget this by accident in a test
     * ```
     */
    private fun Dictionary.setEntry(key: String, value: String?, incrementTimestamp: Boolean = true) {
        this[key] = value
        if (incrementTimestamp) timestamp++
    }

    /**
     * Removes the entry with the given [key] in `this` dictionary. If [incrementTimestamp] is true (the default) then
     * [timestamp] is incremented after the action.
     *
     * This is just a helper function to avoid doing this everywhere:
     * ```
     * this -= key
     * timestamp++  // Ensures we don't forget this by accident in a test
     * ```
     */
    private fun Dictionary.removeEntry(key: String, incrementTimestamp: Boolean = true) {
        this -= key
        if (incrementTimestamp) timestamp++
    }

    /**
     * Asserts that `this` dictionary contains exactly the given `expected` values. Does this by calling both
     * [LwwElementDictionary.entries] and [LwwElementDictionary.get] on every entry.
     */
    private fun <Key, Value, Timestamp : Comparable<Timestamp>, PeerId : Comparable<PeerId>>
        LwwElementDictionary<Key, Value, Timestamp, PeerId>.assertEntries(vararg expected: Pair<Key, Value>) {

        assertEquals(
            expected = expected.toMap(),
            actual = this.entries
        )

        // Arguably we shouldn't do this here: maybe the above is sufficient, and the individual tests for
        // [LwwElementDictionary.get] should be the only place we test that function.
        expected.forEach { (key, value) ->
            assertEquals(
                expected = value,
                actual = this[key]
            )
        }
    }

    @BeforeEach
    fun resetTimestamp() {
        timestamp = 0
    }

    /**
     * Test basic add/remove functionality when working with a single peer.
     */
    @Nested
    inner class SinglePeerActions {
        @Nested
        inner class Upsert {
            @Test
            fun `adds one item to empty dictionary`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.assertEntries("a" to "alan")
            }

            @Test
            fun `adds multiple items to empty dictionary`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl"
                )
            }

            @Test
            fun `ignores upsert if timestamp is earlier than existing entry`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                timestamp = 0
                dictionary["b"] = "bob" // This should be ignored
                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl"
                )
            }

            @Test
            fun `updates existing entry if timestamp is greater than than existing one`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                dictionary.setEntry("b", "bob")
                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to "bob",
                    "c" to "carl"
                )
            }

            @Test
            fun `updates existing entry if timestamp is equal to existing entry (same peer)`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl", incrementTimestamp = false) // Next upsert has same timestamp
                dictionary.setEntry("c", "cheryl")
                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "cheryl"
                )
            }

            @Test
            fun `adds previously deleted item`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.removeEntry("a")
                dictionary.setEntry("a", "anton")
                dictionary.assertEntries(
                    "a" to "anton",
                    "b" to "bella",
                )
            }

            @Test
            fun `handles null value against key`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", null)
                dictionary.setEntry("c", "carl")
                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to null,
                    "c" to "carl",
                )
            }
        }

        @Nested
        inner class Remove {
            @Test
            fun `removes existing item from dictionary`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                dictionary.removeEntry("b")

                dictionary.assertEntries(
                    "a" to "alan",
                    "c" to "carl"
                )
            }

            @Test
            fun `removing non-existing key is no-op`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                dictionary.removeEntry("x") // This key isn't in the dictionary

                dictionary.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl"
                )
            }

            @Test
            fun `handles multiple removals`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("c", "carl")
                dictionary.removeEntry("b")
                dictionary.removeEntry("b")
                timestamp = 0
                dictionary.removeEntry("b")

                dictionary.assertEntries(
                    "a" to "alan",
                    "c" to "carl"
                )
            }
        }

        /**
         * Tests of the (related) [LwwElementDictionary.get] and [LwwElementDictionary.containsKey] functions.
         *
         * Note that `get` is tested in most of the rest of the tests in [assertEntries], but here we test a few edge
         * cases.
         */
        @Nested
        inner class GetAndContainsKey {
            @Test
            fun `non-existing key`() {
                val dictionary = getNewDictionary()
                assertNull(dictionary["missing-key"])
                assertFalse(dictionary.containsKey("missing-key"))
            }

            @Test
            fun `key present with only an upsert`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                assertEquals(
                    expected = "alan",
                    actual = dictionary["a"]
                )
                assertTrue(dictionary.containsKey("a"))
            }

            @Test
            fun `key present but removed`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.removeEntry("a")
                assertNull(dictionary["a"])
                assertFalse(dictionary.containsKey("a"))
            }

            @Test
            fun `key present, removed, then reinstated`() {
                val dictionary = getNewDictionary()
                dictionary.setEntry("a", "alan")
                dictionary.removeEntry("a")
                dictionary.setEntry("a", "amanda")
                assertEquals(
                    expected = "amanda",
                    actual = dictionary["a"]
                )
                assertTrue(dictionary.containsKey("a"))
            }
        }
    }

    /**
     * Test merging multiple CRDTs. In particular, test the three mathematical properties: commutativity, associativity
     * and idempotence.
     */
    @Nested
    inner class Merge {
        @Test
        fun `merges two empty dictionaries`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries()
        }

        @Test
        fun `merges an empty dictionary into a populated dictionary`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryA.setEntry("c", "carl")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries(
                "a" to "alan",
                "b" to "bella",
                "c" to "carl"
            )
        }

        @Test
        fun `merges a populated dictionary into an empty dictionary`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryB.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            dictionaryB.setEntry("c", "carl")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries(
                "a" to "alan",
                "b" to "bella",
                "c" to "carl"
            )
        }

        @Test
        fun `merges two populated dictionaries with no conflicts`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")

            dictionaryB.setEntry("c", "carl")
            dictionaryB.setEntry("d", "della")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries(
                "a" to "alan",
                "b" to "bella",
                "c" to "carl",
                "d" to "della"
            )
        }

        @Test
        fun `merging A into B leaves B untouched`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")

            dictionaryB.setEntry("c", "carl")
            dictionaryB.setEntry("d", "della")

            dictionaryA.merge(dictionaryB)
            dictionaryB.assertEntries(
                "c" to "carl",
                "d" to "della"
            )
        }

        @Test
        fun `merges two dictionaries with item removal`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")

            dictionaryB.setEntry("c", "carl")
            dictionaryB.removeEntry("b")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries(
                "a" to "alan",
                "c" to "carl",
            )
        }

        @Test
        fun `merges two dictionaries with item removal superseded by addition`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")

            dictionaryB.setEntry("c", "carl")
            dictionaryB.removeEntry("b")

            // Reinstate the item previously removed, in the other dictionary
            dictionaryA.setEntry("b", "bella")

            dictionaryA.merge(dictionaryB)
            dictionaryA.assertEntries(
                "a" to "alan",
                "b" to "bella",
                "c" to "carl",
            )
        }

        @Test
        fun `users peer ID to resolve conflicts where timestamps are equal`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")
            val dictionaryC = getNewDictionary("peerC")

            // Add the same key (different values) with the same timestamp, in all three dictionaries
            dictionaryA.setEntry("a", "alan", incrementTimestamp = false)
            dictionaryB.setEntry("a", "anthea", incrementTimestamp = false)
            dictionaryC.setEntry("a", "amanda", incrementTimestamp = false)

            // Merge A into B: the value set in B should be preferred as the peer ID is greater.
            dictionaryB.merge(dictionaryA)
            dictionaryB.assertEntries("a" to "anthea")

            // Merge C into B: the value set in C should be preferred as the peer ID is greater.
            dictionaryB.merge(dictionaryC)
            dictionaryB.assertEntries("a" to "amanda")
        }

        /**
         * Test commutativity, e.g. merging A into B yields the same result as merging B into A.
         */
        @Test
        fun `merge is commutative`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan") // Overridden below
            dictionaryB.setEntry("a", "anthea") // Retained
            dictionaryA.setEntry("b", "bella") // Deleted below
            dictionaryA.setEntry("c", "carl") // Retained
            dictionaryB.removeEntry("b")

            // Work with clones below to ensure the two merges don't affect each other's data.

            val mutated1 = dictionaryA.clone()
            val mutated2 = dictionaryB.clone()

            mutated1.merge(dictionaryB)
            mutated2.merge(dictionaryA)

            listOf(mutated1, mutated2).forEach {
                it.assertEntries(
                    "a" to "anthea",
                    "c" to "carl"
                )
            }
        }

        /**
         * Test associativity, i.e. the order of actions (i.e. the parentheses around the different operations) does not
         * affect the result. e.g. the following two operations end with the same result:
         * * (A merged into B) merged in C
         * * A merged into (B merged in C)
         */
        @Test
        fun `merge is associative`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")
            val dictionaryC = getNewDictionary("peerC")

            dictionaryA.setEntry("a", "alan") // Overridden below
            dictionaryB.setEntry("a", "anthea") // Overridden below
            dictionaryC.setEntry("a", "amanda") // Retained
            dictionaryA.setEntry("b", "bella") // Deleted below
            dictionaryB.removeEntry("b")
            dictionaryC.setEntry("c", "carl") // Overridden below
            dictionaryA.setEntry("c", "cheryl") // Retained

            // Work with clones below to ensure the two scenarios don't affect each other's data.

            // Scenario 1: (A merged into B) merged in C
            val mergedScenario1 = dictionaryB.clone()
                .let { mutatedB ->
                    // Merge A into B
                    mutatedB.merge(dictionaryA)
                    // Merge B (which is now a result of merging A into B) into C
                    dictionaryC.clone().apply { merge(mutatedB) }
                }

            // Scenario 2: A merged into (B merged in C)
            val mergedScenario2 = dictionaryC.clone()
                .apply {
                    // Merge B into C
                    merge(dictionaryB)
                    // Merge A into C (which is now a result of merging B into C)
                    merge(dictionaryA)
                }

            listOf(mergedScenario1, mergedScenario2).forEach {
                it.assertEntries(
                    "a" to "amanda",
                    "c" to "cheryl"
                )
            }
        }

        /**
         * Test idempotence, e.g. merging A into B is the same as merging A into B then A into B again.
         */
        @Test
        fun `merge is idempotent`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan") // Overridden below
            dictionaryB.setEntry("a", "anthea") // Retained
            dictionaryA.setEntry("b", "bella") // Deleted below
            dictionaryA.setEntry("c", "carl") // Retained
            dictionaryB.removeEntry("b")

            // Work with clones below to ensure the two merges don't affect each other's data.
            val mergedOnce = dictionaryB.clone().apply { merge(dictionaryA) }
            val mergedTwice = dictionaryB.clone().apply {
                merge(dictionaryA)
                merge(dictionaryA)
            }

            listOf(mergedOnce, mergedTwice).forEach {
                it.assertEntries(
                    "a" to "anthea",
                    "c" to "carl"
                )
            }
        }
    }

    /**
     * Test that the suggested default concrete implementation of [LwwElementDictionary], namely
     * [DefaultLwwElementDictionary], works as expected. This is just a quick general test: the edge cases are all
     * tested elsewhere.
     */
    @Nested
    inner class DefaultLwwElementDictionaryTest {
        @Test
        fun `default element dictionary operates as expected`() {
            val dictionaryA = DefaultLwwElementDictionary<String, String>(UUID.randomUUID())
            val dictionaryB = DefaultLwwElementDictionary<String, String>(UUID.randomUUID())

            dictionaryA["a"] = "alan" // Overridden below
            dictionaryB["a"] = "anthea" // Retained
            dictionaryA["b"] = "bella" // Deleted below
            dictionaryA["c"] = "carl" // Retained
            dictionaryB -= "b"

            dictionaryA.merge(dictionaryB)

            dictionaryA.assertEntries(
                "a" to "anthea",
                "c" to "carl"
            )
        }
    }

    /**
     * Test [LwwElementDictionary.contentEquals], [LwwElementDictionary.equals] and [LwwElementDictionary.hashCode].
     * Arguably this is overkill, but it gets us to 100% code coverage in the tests.
     */
    @Nested
    inner class Equality {
        @Test
        fun `two dictionaries with same upserts from different peers are considered unequal`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            // Apply the same actions with the same timestamps to the two dictionaries, but each action will have a
            // different peer ID against it so they are considered as different content. We don't merge these
            // dictionaries here.
            listOf(dictionaryA, dictionaryB).forEach { dictionary ->
                timestamp = 0
                dictionary.setEntry("a", "alan")
                dictionary.setEntry("b", "bella")
                dictionary.setEntry("b", "barry")
            }

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two merged dictionaries are considered unequal but same content`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryA.removeEntry("a")
            dictionaryB.setEntry("b", "bob")
            dictionaryB.setEntry("c", "colin")
            dictionaryA.removeEntry("b")

            val aMergedIntoB = dictionaryB.clone().apply { merge(dictionaryA) }
            val bMergedIntoA = dictionaryA.clone().apply { merge(dictionaryB) }

            assertTrue(
                aMergedIntoB.contentEquals(bMergedIntoA),
                "Two dictionaries unexpectedly have different content"
            )
            assertNotEquals(aMergedIntoB, bMergedIntoA)
            assertNotEquals(aMergedIntoB.hashCode(), bMergedIntoA.hashCode())
        }

        @Test
        fun `two dictionaries with different data are considered unequal and having different content`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            // We don't merge these dictionaries here
            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryB.setEntry("c", "colin")
            dictionaryB.setEntry("d", "derek")

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two dictionaries with different number of upserts are considered unequal`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan", incrementTimestamp = false)
            dictionaryB.setEntry("b", "bella")
            dictionaryB.setEntry("c", "colin")

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two dictionaries with different number of removals are considered unequal`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")

            dictionaryA.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            dictionaryB.removeEntry("b")

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two dictionaries with same peer but different removals are considered unequal`() {
            val dictionaryA = getNewDictionary("peerX")
            val dictionaryB = getNewDictionary("peerX")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryA.removeEntry("a")
            timestamp = 0
            dictionaryB.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            dictionaryB.removeEntry("b")

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two dictionaries with single different removal are considered unequal`() {
            val dictionaryA = getNewDictionary("peerX")
            val dictionaryB = getNewDictionary("peerX")

            dictionaryA.removeEntry("a")
            dictionaryB.removeEntry("a") // Has a different timestamp to the above

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `dictionary considered equal to itself`() {
            val dictionaryA = getNewDictionary("peerA")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")

            assertTrue(
                dictionaryA.contentEquals(dictionaryA),
                "Dictionaries unexpectedly has different content to itself"
            )
            assertEquals(dictionaryA, dictionaryA)
            assertEquals(dictionaryA.hashCode(), dictionaryA.hashCode())
        }

        @Test
        fun `dictionary considered unequal to object of a different class`() {
            val dictionaryA = getNewDictionary("peerA")

            assertNotEquals(dictionaryA as Any, "this is not a dictionary")
            assertNotEquals(dictionaryA.hashCode(), "this is not a dictionary".hashCode())
        }

        @Test
        fun `two identical dictionaries are considered equal`() {
            val dictionaryA = getNewDictionary("peerX")
            val dictionaryB = getNewDictionary("peerX")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryA.removeEntry("a")
            timestamp = 0
            dictionaryB.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            dictionaryB.removeEntry("a")

            assertTrue(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have different content"
            )
            assertEquals(dictionaryA, dictionaryB)
            assertEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }

        @Test
        fun `two dictionaries with same upserts at different times are considered unequal`() {
            val dictionaryA = getNewDictionary("peerX")
            val dictionaryB = getNewDictionary("peerX")

            dictionaryA.setEntry("a", "alan")
            dictionaryA.setEntry("b", "bella")
            dictionaryA.setEntry("c", "colin")
            timestamp = 0
            dictionaryB.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            timestamp = 5
            dictionaryA.setEntry("c", "colin")

            assertFalse(
                dictionaryA.contentEquals(dictionaryB),
                "Two dictionaries unexpectedly have same content"
            )
            assertNotEquals(dictionaryA, dictionaryB)
            assertNotEquals(dictionaryA.hashCode(), dictionaryB.hashCode())
        }
    }

    /**
     * Test [LwwElementDictionary.toString] (this is probably overkill too, but keeps the code coverage at 100%).
     */
    @Nested
    inner class ToString {
        @Test
        fun `toString for empty dictionary`() {
            val dictionary = getNewDictionary("peerA")

            assertEquals(
                expected = "LwwElementDictionary(peerId=peerA, upserts={}, removals={})",
                actual = dictionary.toString()
            )
        }

        @Test
        fun `toString for populated dictionary`() {
            val dictionaryA = getNewDictionary("peerA")
            val dictionaryB = getNewDictionary("peerB")
            dictionaryA.setEntry("a", "alan")
            dictionaryB.setEntry("b", "bella")
            dictionaryA.removeEntry("b")
            dictionaryA.merge(dictionaryB)

            assertEquals(
                expected =
                    "LwwElementDictionary(peerId=peerA, " +
                        "upserts={a=Upsert(value=alan, timestamp=0, peerId=peerA), " +
                        "b=Upsert(value=bella, timestamp=1, peerId=peerB)}, " +
                        "removals={b=Removal(timestamp=2, peerId=peerA)})",
                actual = dictionaryA.toString()
            )
        }
    }
}