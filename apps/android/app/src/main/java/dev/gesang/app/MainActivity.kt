package dev.gesang.app

import android.content.ActivityNotFoundException
import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.browser.customtabs.CustomTabsIntent
import androidx.compose.runtime.LaunchedEffect
import androidx.core.net.toUri
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import dev.gesang.app.ui.GesangApp
import dev.gesang.app.ui.theme.GesangTheme

class MainActivity : ComponentActivity() {
    private val model by viewModels<AppViewModel>()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        handleIntent(intent)
        setContent {
            val state = model.state.collectAsStateWithLifecycle().value
            LaunchedEffect(state.browserDestination) {
                state.browserDestination?.let { destination ->
                    try {
                        CustomTabsIntent.Builder()
                            .build()
                            .launchUrl(this@MainActivity, destination.toUri())
                    } catch (_: ActivityNotFoundException) {
                        model.browserUnavailable()
                    }
                    model.browserOpened()
                }
            }
            GesangTheme { GesangApp(state, model) }
        }
    }

    override fun onStart() {
        super.onStart()
        model.foreground()
    }

    override fun onStop() {
        model.background()
        super.onStop()
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleIntent(intent)
    }

    private fun handleIntent(intent: Intent) {
        val callback = intent.data
        if (callback?.scheme == "dev.gesang.app" && callback.path == "/oauth/callback") {
            model.completeSignIn(callback.toString())
        } else if (intent.hasExtra("eventId")) {
            val agentId = intent.getStringExtra("agentId")
            val goalId = intent.getStringExtra("goalId")
            if (agentId != null && goalId != null) model.openNotification(agentId, goalId)
        }
    }
}
