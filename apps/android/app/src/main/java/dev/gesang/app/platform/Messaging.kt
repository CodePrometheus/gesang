package dev.gesang.app.platform

import android.app.NotificationManager
import android.content.Context
import androidx.core.app.NotificationManagerCompat
import com.google.firebase.FirebaseApp
import com.google.firebase.messaging.FirebaseMessaging
import dev.gesang.app.BuildConfig
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import org.json.JSONObject

internal enum class PushPhase {
    UNAVAILABLE,
    OFF,
    PERMISSION_REQUIRED,
    REGISTERING,
    SERVER_PENDING,
    REGISTERED,
    REVOCATION_PENDING,
}

internal data class PushPreference(
    val deviceId: String = UUID.randomUUID().toString(),
    val revision: String = UUID.randomUUID().toString(),
    val enabled: Boolean = false,
    val revocationPending: Boolean = false,
    val registeredInstallationId: String? = null,
)

/**
 * Provider callbacks carry no credentials and wake only the active account's registration
 * coordinator.
 */
internal object MessagingEvents {
    val lock = Any()
    private val revision = MutableStateFlow(0L)
    val changes = revision.asStateFlow()

    fun changed() {
        revision.value += 1
    }
}

/** Persisted owner preference is separate from OS permission and server acknowledgment. */
internal class MessagingStore(private val context: Context, private val secure: SecureStore) {
    fun preference(account: Account): PushPreference =
        synchronized(MessagingEvents.lock) {
            val data = secure.read(recordKey(account)) ?: return@synchronized PushPreference()
            val record = JSONObject(data.toString(Charsets.UTF_8))
            check(record.getInt("version") == 1) { "Unsupported notification preference version." }
            PushPreference(
                record.getString("deviceId"),
                record.getString("revision"),
                record.getBoolean("enabled"),
                record.getBoolean("revocationPending"),
                if (record.isNull("registeredInstallationId")) null
                else record.getString("registeredInstallationId"),
            )
        }

    fun save(account: Account, value: PushPreference) =
        synchronized(MessagingEvents.lock) {
            val record =
                JSONObject()
                    .put("version", 1)
                    .put("deviceId", value.deviceId)
                    .put("revision", value.revision)
                    .put("enabled", value.enabled)
                    .put("revocationPending", value.revocationPending)
                    .put(
                        "registeredInstallationId",
                        value.registeredInstallationId ?: JSONObject.NULL,
                    )
            secure.write(recordKey(account), record.toString().toByteArray(Charsets.UTF_8))
        }

    fun disableLocal(account: Account): PushPreference =
        synchronized(MessagingEvents.lock) {
            val previous = preference(account)
            val updated =
                previous.copy(
                    revision = UUID.randomUUID().toString(),
                    enabled = false,
                    revocationPending =
                        previous.enabled ||
                            previous.revocationPending ||
                            previous.registeredInstallationId != null,
                )
            save(account, updated)
            context.getSystemService(NotificationManager::class.java).cancelAll()
            updated
        }

    fun installationId(): String? =
        synchronized(MessagingEvents.lock) {
            secure.read("messaging_registration")?.toString(Charsets.UTF_8)
        }

    fun systemPermissionGranted(): Boolean =
        NotificationManagerCompat.from(context).areNotificationsEnabled()

    fun projectId(): String? =
        if (!BuildConfig.MESSAGING_CONFIGURED) null else FirebaseApp.getInstance().options.projectId

    suspend fun register() {
        check(BuildConfig.MESSAGING_CONFIGURED) {
            "Remote messaging is not configured for this build."
        }
        suspendCancellableCoroutine { continuation ->
            FirebaseMessaging.getInstance()
                .register()
                .addOnSuccessListener { if (continuation.isActive) continuation.resume(Unit) }
                .addOnFailureListener { failure ->
                    if (continuation.isActive) continuation.resumeWithException(failure)
                }
        }
    }

    fun stopProvider() {
        if (BuildConfig.MESSAGING_CONFIGURED) {
            FirebaseMessaging.getInstance().isAutoInitEnabled = false
            FirebaseMessaging.getInstance().unregister()
        }
    }

    private fun recordKey(account: Account): String =
        accountRecordKey(account.endpoint, account.ownerId, "notifications")
}
