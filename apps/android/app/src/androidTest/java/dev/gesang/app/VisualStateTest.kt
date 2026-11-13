package dev.gesang.app

import android.graphics.Bitmap
import android.graphics.Color
import android.view.WindowManager
import android.view.accessibility.AccessibilityNodeInfo
import android.view.inspector.WindowInspector
import androidx.activity.ComponentActivity
import androidx.activity.SystemBarStyle
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.lifecycle.ViewModelProvider
import androidx.test.core.app.ActivityScenario
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.SdkSuppress
import androidx.test.platform.app.InstrumentationRegistry
import com.google.android.apps.common.testing.accessibility.framework.integrations.espresso.AccessibilityValidator
import dev.gesang.app.ui.GesangApp
import dev.gesang.app.ui.theme.GesangTheme
import dev.gesang.nativeclient.*
import java.io.File
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Visual fixtures exercise product composables only; they do not authenticate or execute cloud
 * work.
 */
@RunWith(AndroidJUnit4::class)
@SdkSuppress(minSdkVersion = 34)
class VisualStateTest {
    @Test fun lightChatProfileAndPermissionPassAccessibilityChecks() = render(false)

    @Test fun darkChatProfileAndPermissionPassAccessibilityChecks() = render(true)

    private fun render(dark: Boolean) {
        // Espresso 3.7 calls a hidden InputManager method removed in API 37. Use public
        // UiAutomation actions and the same Accessibility Test Framework validator directly.
        ActivityScenario.launch(ComponentActivity::class.java).use { scenario ->
            lateinit var activity: ComponentActivity
            scenario.onActivity { current ->
                activity = current
                val bar =
                    if (dark) SystemBarStyle.dark(Color.TRANSPARENT)
                    else SystemBarStyle.light(Color.TRANSPARENT, Color.TRANSPARENT)
                activity.enableEdgeToEdge(statusBarStyle = bar, navigationBarStyle = bar)
                activity.window.setSoftInputMode(
                    WindowManager.LayoutParams.SOFT_INPUT_STATE_ALWAYS_HIDDEN
                )
                val model = ViewModelProvider(activity)[AppViewModel::class.java]
                val state = fixture()
                activity.setContent { GesangTheme(darkTheme = dark) { GesangApp(state, model) } }
            }
            waitForIdle()
            checkAccessibility(scenario)
            capture("fixture-chat-${if (dark) "dark" else "light"}")
            click(activity.getString(R.string.more_options))
            click(activity.getString(R.string.my_agent))
            checkAccessibility(scenario)
            capture("fixture-profile-${if (dark) "dark" else "light"}")
            click(activity.getString(R.string.permission_title))
            checkAccessibility(scenario)
            capture("fixture-permission-${if (dark) "dark" else "light"}")
        }
    }

    private fun checkAccessibility(scenario: ActivityScenario<ComponentActivity>) {
        waitForIdle()
        scenario.onActivity {
            val validator = AccessibilityValidator().setRunChecksFromRootView(true)
            WindowInspector.getGlobalWindowViews().filter { it.isShown }.forEach(validator::check)
        }
    }

    private fun waitForIdle() {
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        instrumentation.waitForIdleSync()
        instrumentation.uiAutomation.waitForIdle(100L, 5_000L)
    }

    private fun click(label: String) {
        waitForIdle()
        val automation = InstrumentationRegistry.getInstrumentation().uiAutomation
        val root = requireNotNull(automation.rootInActiveWindow)
        val nodes = ArrayDeque<AccessibilityNodeInfo>()
        nodes.add(root)
        while (nodes.isNotEmpty()) {
            val node = nodes.removeFirst()
            if (
                node.isVisibleToUser &&
                    (node.text?.toString() == label || node.contentDescription?.toString() == label)
            ) {
                var clickable: AccessibilityNodeInfo? = node
                while (clickable != null && !clickable.isClickable) clickable = clickable.parent
                check(clickable?.performAction(AccessibilityNodeInfo.ACTION_CLICK) == true) {
                    "The labeled control could not be activated: $label"
                }
                waitForIdle()
                return
            }
            for (index in 0 until node.childCount) node.getChild(index)?.let(nodes::add)
        }
        error("The labeled control was not found: $label")
    }

    private fun capture(name: String) {
        waitForIdle()
        val instrumentation = InstrumentationRegistry.getInstrumentation()
        val bitmap = requireNotNull(instrumentation.uiAutomation.takeScreenshot())
        val destination = File(instrumentation.targetContext.filesDir, "$name.png")
        destination.outputStream().use { bitmap.compress(Bitmap.CompressFormat.PNG, 100, it) }
    }

    private fun fixture(): AppState {
        val agent = "ad5a7216-88e6-4169-852e-0681c5b05e24"
        val grant =
            GrantView(
                "ffba70b2-0a67-4155-bd69-71c2a5907bba",
                agent,
                1,
                Permission.FULL_ACCESS,
                emptyList(),
                null,
            )
        val now = System.currentTimeMillis()
        val goal =
            GoalView(
                goalId = "ca1cd9bb-068c-4745-8937-0dbe24141a46",
                conversationId = 1uL,
                version = 3uL,
                phase = GoalPhase.WAITING_USER,
                objective = "帮我看看这次改动。",
                completionCriteria = emptyList(),
                waitingReason = "需要继续检查哪些部分？",
                questionId = "fixture-question",
                waitingUntilMs = null,
                resultSummary = null,
                evidence = emptyList(),
                updatedAtMs = now.toDouble(),
                synchronizedAtMs = now,
            )
        fun message(role: MessageRole, text: String) =
            MessageView(
                role,
                listOf(ContentBlock(ContentKind.TEXT, text, null, null, null, null)),
                null,
                null,
                false,
                false,
                now.toDouble(),
                null,
                null,
            )
        val entries =
            listOf(
                TranscriptEntryView(
                    1uL,
                    1uL,
                    "user",
                    listOf(message(MessageRole.USER, goal.objective)),
                    null,
                ),
                TranscriptEntryView(
                    2uL,
                    1uL,
                    "assistant",
                    listOf(
                        message(
                            MessageRole.ASSISTANT,
                            "先查看本次改动，再决定验证范围。\n\n```sh\ngit diff --stat\n```\n\n需要我按功能分组继续整理吗？",
                        )
                    ),
                    null,
                ),
            )
        return AppState(
            endpoint = "https://gesang.example",
            session = SessionView("c87f69aa-65f9-4ecd-a077-786bfc621b39", agent, grant, now),
            connection = Connection.ONLINE,
            goals = listOf(goal),
            selectedGoalId = goal.goalId,
            transcript = entries,
            observation = ObservationConnection.LIVE,
            transcriptSynchronizedAtMs = now,
        )
    }
}
