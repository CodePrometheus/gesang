package dev.gesang.app.platform

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import dev.gesang.nativeclient.TranscriptCacheMetadata
import dev.gesang.nativeclient.TranscriptEntryView
import dev.gesang.nativeclient.decodeTranscriptEntry
import dev.gesang.nativeclient.decodeTranscriptMetadata
import dev.gesang.nativeclient.encodeTranscriptEntry
import dev.gesang.nativeclient.encodeTranscriptMetadata

internal data class CachedTranscript(
    val metadata: TranscriptCacheMetadata,
    val entries: List<TranscriptEntryView>,
)

/** Independent encrypted rows prevent draft writes from rewriting every cached conversation. */
internal class TranscriptStore(context: Context, private val secure: SecureStore) :
    SQLiteOpenHelper(context, "transcript-v1.db", null, 1) {
    init {
        setWriteAheadLoggingEnabled(true)
    }

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(
            "CREATE TABLE records (namespace TEXT NOT NULL, record TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(namespace, record))"
        )
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        error("Unsupported transcript cache migration: $oldVersion to $newVersion.")
    }

    fun save(
        namespace: String,
        metadata: TranscriptCacheMetadata,
        entries: List<TranscriptEntryView>,
    ) {
        val database = writableDatabase
        database.beginTransaction()
        try {
            entries.forEach { entry ->
                write(database, namespace, entry.entryId.toString(), encodeTranscriptEntry(entry))
            }
            write(database, namespace, "metadata", encodeTranscriptMetadata(metadata))
            database.setTransactionSuccessful()
        } finally {
            database.endTransaction()
        }
    }

    fun read(namespace: String): CachedTranscript? {
        val database = readableDatabase
        database.beginTransactionNonExclusive()
        try {
            val header = read(database, namespace, "metadata") ?: return null
            val metadata = decodeTranscriptMetadata(header)
            val entries =
                metadata.entryIds.map { id ->
                    val encoded =
                        read(database, namespace, id.toString())
                            ?: error("A cached transcript entry is missing.")
                    decodeTranscriptEntry(encoded).also {
                        check(it.entryId == id) {
                            "A cached transcript entry has an unexpected identity."
                        }
                    }
                }
            database.setTransactionSuccessful()
            return CachedTranscript(metadata, entries)
        } finally {
            database.endTransaction()
        }
    }

    private fun write(
        database: SQLiteDatabase,
        namespace: String,
        record: String,
        plaintext: ByteArray,
    ) {
        try {
            val values =
                ContentValues().apply {
                    put("namespace", namespace)
                    put("record", record)
                    put("value", secure.encryptRecord("$namespace:$record", plaintext))
                }
            check(
                database.insertWithOnConflict(
                    "records",
                    null,
                    values,
                    SQLiteDatabase.CONFLICT_REPLACE,
                ) != -1L
            ) {
                "A cached transcript record could not be persisted."
            }
        } finally {
            plaintext.fill(0)
        }
    }

    private fun read(database: SQLiteDatabase, namespace: String, record: String): ByteArray? =
        database
            .query(
                "records",
                arrayOf("value"),
                "namespace=? AND record=?",
                arrayOf(namespace, record),
                null,
                null,
                null,
            )
            .use { cursor ->
                if (!cursor.moveToFirst()) null
                else secure.decryptRecord("$namespace:$record", cursor.getBlob(0))
            }
}
