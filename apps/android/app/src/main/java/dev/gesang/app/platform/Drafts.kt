package dev.gesang.app.platform

import android.util.Base64
import androidx.annotation.MainThread
import dev.gesang.nativeclient.PendingCommand
import java.util.UUID
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject

/** A revision identifies edits even when the text later returns to the same value. */
internal data class StoredDraft(val revision: String, val text: String) {
    fun encode(): ByteArray =
        JSONObject()
            .put("version", 1)
            .put("revision", revision)
            .put("text", text)
            .toString()
            .toByteArray(Charsets.UTF_8)

    companion object {
        fun empty() = StoredDraft(UUID.randomUUID().toString(), "")

        fun decode(bytes: ByteArray): StoredDraft {
            val record = JSONObject(bytes.toString(Charsets.UTF_8))
            check(record.getInt("version") == 1) { "Unsupported draft record version." }
            return StoredDraft(record.getString("revision"), record.getString("text"))
        }
    }
}

/** Local presentation metadata never changes the opaque execution request. */
internal data class StoredDelivery(
    val command: PendingCommand,
    val draftKey: String?,
    val draftRevision: String?,
    val draftGoalId: String?,
    val text: String?,
) {
    fun encode(): ByteArray =
        JSONObject()
            .put("version", 1)
            .put("command", Base64.encodeToString(command.encode(), Base64.NO_WRAP))
            .put("draftKey", draftKey ?: JSONObject.NULL)
            .put("draftRevision", draftRevision ?: JSONObject.NULL)
            .put("draftGoalId", draftGoalId ?: JSONObject.NULL)
            .put("text", text ?: JSONObject.NULL)
            .toString()
            .toByteArray(Charsets.UTF_8)

    companion object {
        fun decode(bytes: ByteArray): StoredDelivery {
            val record = JSONObject(bytes.toString(Charsets.UTF_8))
            check(record.getInt("version") == 1) { "Unsupported delivery record version." }
            return StoredDelivery(
                PendingCommand.restore(Base64.decode(record.getString("command"), Base64.NO_WRAP)),
                record.optionalString("draftKey"),
                record.optionalString("draftRevision"),
                record.optionalString("draftGoalId"),
                record.optionalString("text"),
            )
        }

        private fun JSONObject.optionalString(name: String): String? =
            if (isNull(name)) null else getString(name)
    }
}

/** Coalesce per draft, preserving ordering between user edits and delivery receipts. */
@MainThread
internal class DraftWriter(
    scope: CoroutineScope,
    private val store: SecureStore,
    onFailure: () -> Unit,
) {
    private sealed interface Mutation {
        data class Write(val draft: StoredDraft) : Mutation

        data class Clear(val revision: String) : Mutation
    }

    private val pending = linkedMapOf<String, Mutation>()
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private val barriers = mutableListOf<CompletableDeferred<Unit>>()
    private var inFlight = false

    private val worker = scope.launch {
        while (wake.receiveCatching().isSuccess) {
            while (pending.isNotEmpty()) {
                val batch = pending.toMap()
                pending.clear()
                inFlight = true
                try {
                    withContext(Dispatchers.IO) {
                        for ((key, mutation) in batch) when (mutation) {
                            is Mutation.Write -> store.write(key, mutation.draft.encode())
                            is Mutation.Clear -> {
                                val current = store.read(key)?.let(StoredDraft::decode)
                                if (current?.revision == mutation.revision)
                                    store.write(key, StoredDraft.empty().encode())
                            }
                        }
                    }
                } catch (failure: kotlinx.coroutines.CancellationException) {
                    throw failure
                } catch (failure: Exception) {
                    for ((key, mutation) in batch) pending.putIfAbsent(key, mutation)
                    barriers.forEach { it.completeExceptionally(failure) }
                    barriers.clear()
                    onFailure()
                    break
                } finally {
                    inFlight = false
                }
            }
            if (pending.isEmpty()) {
                barriers.forEach { it.complete(Unit) }
                barriers.clear()
            }
        }
    }

    fun write(key: String, draft: StoredDraft) {
        pending[key] = Mutation.Write(draft)
        wake.trySend(Unit)
    }

    fun clearIfUnchanged(key: String, revision: String) {
        val queued = pending[key]
        if (queued is Mutation.Write && queued.draft.revision != revision) return
        pending[key] =
            if (queued is Mutation.Write) Mutation.Write(StoredDraft.empty())
            else Mutation.Clear(revision)
        wake.trySend(Unit)
    }

    suspend fun flush() {
        if (pending.isEmpty() && !inFlight) return
        val barrier = CompletableDeferred<Unit>()
        barriers += barrier
        wake.trySend(Unit)
        barrier.await()
    }
}
