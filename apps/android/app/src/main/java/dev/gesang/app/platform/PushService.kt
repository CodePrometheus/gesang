package dev.gesang.app.platform

import android.Manifest
import android.annotation.SuppressLint
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import androidx.core.app.NotificationCompat
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import dev.gesang.app.MainActivity
import dev.gesang.app.R

/** Render generic updates only; authenticated content is fetched after opening the app. */
// The legacy lint rule only recognizes onNewToken; Messaging 26 uses onRegistered with FIDs.
@SuppressLint("MissingFirebaseInstanceTokenRefresh")
class PushService : FirebaseMessagingService() {
    override fun onMessageReceived(message: RemoteMessage) {
        val eventId = message.data["eventId"]?.takeIf { it.isNotBlank() } ?: return
        synchronized(MessagingEvents.lock) {
            val store = SecureStore(this)
            val account = store.account() ?: return
            if (
                account.ownerId == null ||
                    account.agentId != message.data["agentId"] ||
                    !MessagingStore(this, store).preference(account).enabled
            )
                return
            if (
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                    checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) !=
                        PackageManager.PERMISSION_GRANTED
            )
                return
            val intent =
                Intent(this, MainActivity::class.java)
                    .setData(
                        Uri.Builder()
                            .scheme("dev.gesang.app")
                            .authority("event")
                            .appendPath(eventId)
                            .build()
                    )
                    .putExtra("eventId", eventId)
                    .putExtra("goalId", message.data["goalId"])
                    .putExtra("agentId", message.data["agentId"])
                    .addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP)
            val pending =
                PendingIntent.getActivity(
                    this,
                    0,
                    intent,
                    PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
                )
            val notification =
                NotificationCompat.Builder(this, "task_updates")
                    .setSmallIcon(R.drawable.ic_gesang)
                    .setContentTitle(getString(R.string.notification_title))
                    .setContentText(getString(R.string.notification_text))
                    .setContentIntent(pending)
                    .setAutoCancel(true)
                    .setOnlyAlertOnce(true)
                    .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
                    .build()
            getSystemService(NotificationManager::class.java).notify(eventId, 0, notification)
        }
    }

    override fun onRegistered(installationId: String) {
        synchronized(MessagingEvents.lock) {
            SecureStore(this)
                .write("messaging_registration", installationId.toByteArray(Charsets.UTF_8))
            MessagingEvents.changed()
        }
    }

    override fun onUnregistered(installationId: String) {
        synchronized(MessagingEvents.lock) {
            val store = SecureStore(this)
            if (store.read("messaging_registration")?.toString(Charsets.UTF_8) == installationId) {
                store.clear("messaging_registration")
                MessagingEvents.changed()
            }
        }
    }
}
