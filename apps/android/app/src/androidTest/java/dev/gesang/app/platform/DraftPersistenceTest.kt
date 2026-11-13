package dev.gesang.app.platform

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class DraftPersistenceTest {
    private val store
        get() = SecureStore(InstrumentationRegistry.getInstrumentation().targetContext)

    @Test
    fun fastEditsToDifferentConversationsAreBothDurable() = runBlocking {
        val storage = store
        val job = SupervisorJob()
        val scope = CoroutineScope(Dispatchers.Main + job)
        val firstKey = UUID.randomUUID().toString()
        val secondKey = UUID.randomUUID().toString()
        try {
            val writer =
                withContext(Dispatchers.Main) {
                    DraftWriter(scope, storage) {}
                        .also {
                            it.write(firstKey, StoredDraft("first", "First conversation draft"))
                            it.write(secondKey, StoredDraft("second", "Second conversation draft"))
                        }
                }
            withContext(Dispatchers.Main) { writer.flush() }
            assertEquals(
                "First conversation draft",
                StoredDraft.decode(storage.read(firstKey)!!).text,
            )
            assertEquals(
                "Second conversation draft",
                StoredDraft.decode(storage.read(secondKey)!!).text,
            )
        } finally {
            job.cancelAndJoin()
        }
    }

    @Test
    fun receiptForOlderTextDoesNotClearANewerDraft() = runBlocking {
        val storage = store
        val job = SupervisorJob()
        val scope = CoroutineScope(Dispatchers.Main + job)
        val key = UUID.randomUUID().toString()
        storage.write(key, StoredDraft("submitted", "Submitted message").encode())
        try {
            val writer =
                withContext(Dispatchers.Main) {
                    DraftWriter(scope, storage) {}
                        .also {
                            it.write(key, StoredDraft("newer", "New message written while waiting"))
                            it.clearIfUnchanged(key, "submitted")
                        }
                }
            withContext(Dispatchers.Main) { writer.flush() }
            val persisted = StoredDraft.decode(storage.read(key)!!)
            assertEquals("newer", persisted.revision)
            assertEquals("New message written while waiting", persisted.text)
        } finally {
            job.cancelAndJoin()
        }
    }

    @Test
    fun acknowledgedQueuedDraftDoesNotResurrectAnEarlierStoredValue() = runBlocking {
        val storage = store
        val job = SupervisorJob()
        val scope = CoroutineScope(Dispatchers.Main + job)
        val key = UUID.randomUUID().toString()
        storage.write(key, StoredDraft("previous", "Earlier unfinished text").encode())
        try {
            val writer =
                withContext(Dispatchers.Main) {
                    DraftWriter(scope, storage) {}
                        .also {
                            it.write(key, StoredDraft("submitted", "The accepted message"))
                            it.clearIfUnchanged(key, "submitted")
                        }
                }
            withContext(Dispatchers.Main) { writer.flush() }
            val persisted = storage.read(key)
            assertNotNull(persisted)
            assertEquals("", StoredDraft.decode(persisted!!).text)
        } finally {
            job.cancelAndJoin()
        }
    }
}
