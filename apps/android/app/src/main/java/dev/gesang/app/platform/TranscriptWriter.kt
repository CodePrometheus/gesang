package dev.gesang.app.platform

import dev.gesang.nativeclient.TranscriptCacheMetadata
import dev.gesang.nativeclient.TranscriptEntryView
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Coalesce wake signals and changed rows, never another conversation's queued state. */
internal class TranscriptWriter(
    scope: CoroutineScope,
    private val store: TranscriptStore,
    private val onFailure: () -> Unit,
) {
    private data class Pending(
        val metadata: TranscriptCacheMetadata,
        val entries: Map<ULong, TranscriptEntryView>,
    )

    private val pending = linkedMapOf<String, Pending>()
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private val barriers = mutableListOf<CompletableDeferred<Unit>>()
    private var inFlight = false

    init {
        scope.launch {
            for (signal in wake) {
                while (pending.isNotEmpty()) {
                    val batch = pending.toMap()
                    pending.clear()
                    inFlight = true
                    try {
                        withContext(Dispatchers.IO) {
                            for ((namespace, state) in batch) store.save(
                                namespace,
                                state.metadata,
                                state.entries.values.toList(),
                            )
                        }
                    } catch (failure: CancellationException) {
                        throw failure
                    } catch (failure: Exception) {
                        for ((namespace, state) in batch) {
                            val newer = pending[namespace]
                            pending[namespace] =
                                if (newer == null) state
                                else Pending(newer.metadata, state.entries + newer.entries)
                        }
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
    }

    fun write(
        namespace: String,
        metadata: TranscriptCacheMetadata,
        changed: List<TranscriptEntryView>,
    ) {
        val entries = pending[namespace]?.entries.orEmpty() + changed.associateBy { it.entryId }
        pending[namespace] = Pending(metadata, entries)
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
