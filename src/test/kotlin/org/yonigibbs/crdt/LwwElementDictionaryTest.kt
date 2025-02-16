package org.yonigibbs.crdt

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

// TODO: test for null values.
private typealias Dictionary = LwwElementDictionary<String, String?, Int, String>

class LwwElementDictionaryTest {
    var timestamp = 0

    private fun getNewDictionary(peerId: String = "peer1") = Dictionary(peerId) { timestamp }

    private fun Dictionary.setEntry(key: String, value: String?, incrementTimestamp: Boolean = true) {
        this[key] = value
        if (incrementTimestamp) timestamp++
    }

    private fun Dictionary.removeEntry(key: String, incrementTimestamp: Boolean = true) {
        this -= key
        if (incrementTimestamp) timestamp++
    }

    private fun Dictionary.assertEntries(vararg expected: Pair<String, String?>) {
        assertEquals(
            expected = expected.toMap(),
            actual = this.entries
        )

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
    inner class SimpleSinglePeerActions {
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
            fun `ignores add if timestamp is earlier than existing entry`() {
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
        }
    }

    /**
     * Test merging multiple CRDTs. In particular, test the three mathematical properties: commutativity, associativity
     * and idempotence.
     */
    @Nested
    inner class Merge {
        /**
         * Some basic simple merges
         */
        @Nested
        inner class SimpleMerge {
            @Test
            fun `merges two empty dictionaries`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries()
            }

            @Test
            fun `merges an empty dictionary into a populated dictionary`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary1.setEntry("a", "alan")
                dictionary1.setEntry("b", "bella")
                dictionary1.setEntry("c", "carl")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl"
                )

                // TODO: add a test to make sure merging didn't mutate the other dictionary
            }

            @Test
            fun `merges a populated dictionary into an empty dictionary`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary2.setEntry("a", "alan")
                dictionary2.setEntry("b", "bella")
                dictionary2.setEntry("c", "carl")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl"
                )
            }

            @Test
            fun `merges two populated dictionaries with no conflicts`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary1.setEntry("a", "alan")
                dictionary1.setEntry("b", "bella")

                dictionary2.setEntry("c", "carl")
                dictionary2.setEntry("d", "della")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl",
                    "d" to "della"
                )
            }

            @Test
            fun `merges two dictionaries with item removal`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary1.setEntry("a", "alan")
                dictionary1.setEntry("b", "bella")

                dictionary2.setEntry("c", "carl")
                dictionary2.removeEntry("b")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries(
                    "a" to "alan",
                    "c" to "carl",
                )
            }

            @Test
            fun `merges two dictionaries with item removal superseded by addition`() {
                val dictionary1 = getNewDictionary("peer1")
                val dictionary2 = getNewDictionary("peer2")

                dictionary1.setEntry("a", "alan")
                dictionary1.setEntry("b", "bella")

                dictionary2.setEntry("c", "carl")
                dictionary2.removeEntry("b")

                // Reinstate the item previously removed, in the other dictionary
                dictionary1.setEntry("b", "bella")

                dictionary1.merge(dictionary2)
                dictionary1.assertEntries(
                    "a" to "alan",
                    "b" to "bella",
                    "c" to "carl",
                )
            }
        }

        /**
         * Test how conflicts are handled when merging
         */
        inner class ConflictHandling {

        }
    }
}