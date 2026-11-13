package dev.gesang.app.platform

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import dev.gesang.nativeclient.TranscriptCacheMetadata
import dev.gesang.nativeclient.TranscriptEntryView
import java.util.UUID
import javax.crypto.AEADBadTagException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Real device-keystore encryption and SQLite membership, using explicit test-only source records.
 */
@RunWith(AndroidJUnit4::class)
class TranscriptPersistenceTest {
    @Test
    fun restoredMembershipDoesNotInventACompleteHistoryFromOlderRows() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val secure = SecureStore(context)
        val namespace = UUID.randomUUID().toString()
        val first = TranscriptEntryView(1uL, 1uL, "test-user", emptyList(), "First test record")
        val second =
            TranscriptEntryView(2uL, 1uL, "test-assistant", emptyList(), "Second test record")
        TranscriptStore(context, secure).use { store ->
            val metadata =
                TranscriptCacheMetadata(1uL, listOf(1uL, 2uL), null, null, false, 1234L, null)
            store.save(namespace, metadata, listOf(first, second))
            store.save(namespace, metadata.copy(entryIds = listOf(2uL)), emptyList())
        }
        TranscriptStore(context, SecureStore(context)).use { restored ->
            val snapshot = requireNotNull(restored.read(namespace))
            assertEquals(listOf(2uL), snapshot.entries.map { it.entryId })
            assertEquals(1234L, snapshot.metadata.synchronizedAtMs)
            assertEquals("Second test record", snapshot.entries.single().details)
        }
    }

    @Test
    fun encryptedRecordsCannotBeReboundToAnotherOwnerNamespace() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val secure = SecureStore(context)
        val ciphertext =
            secure.encryptRecord("owner-a:entry-1", "Test-only private text".toByteArray())
        assertThrows(AEADBadTagException::class.java) {
            secure.decryptRecord("owner-b:entry-1", ciphertext)
        }
    }
}
