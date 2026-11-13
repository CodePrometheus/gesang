package dev.gesang.app

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.os.Build

class GesangApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel =
                NotificationChannel(
                    "task_updates",
                    getString(R.string.notification_channel),
                    NotificationManager.IMPORTANCE_DEFAULT,
                )
            getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
        }
    }
}
