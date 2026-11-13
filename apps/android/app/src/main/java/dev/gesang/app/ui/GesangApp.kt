package dev.gesang.app.ui

import android.Manifest
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.provider.Settings
import android.text.format.DateUtils
import androidx.activity.compose.BackHandler
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.annotation.DrawableRes
import androidx.annotation.StringRes
import androidx.compose.foundation.background
import androidx.compose.foundation.interaction.collectIsDraggedAsState
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import dev.gesang.app.AppState
import dev.gesang.app.AppViewModel
import dev.gesang.app.BuildConfig
import dev.gesang.app.Connection
import dev.gesang.app.ObservationConnection
import dev.gesang.app.R
import dev.gesang.app.platform.PushPhase
import dev.gesang.app.ui.theme.LocalSurfaceColors
import dev.gesang.nativeclient.GoalPhase
import dev.gesang.nativeclient.GoalView
import dev.gesang.nativeclient.Permission
import kotlinx.coroutines.launch

private enum class Screen {
    CHAT,
    PROFILE,
    LEARNING,
}

private enum class Sheet {
    PERMISSION,
    ACTIVITY,
    CONNECTION,
    NOTIFICATIONS,
    SIGN_OUT,
    REVOKE,
    STOP,
    RESUME,
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun GesangApp(state: AppState, model: AppViewModel) {
    var screen by rememberSaveable { mutableStateOf(Screen.CHAT) }
    var sheet by rememberSaveable { mutableStateOf<Sheet?>(null) }
    var promptedOwner by rememberSaveable { mutableStateOf<String?>(null) }
    val drawer = rememberDrawerState(DrawerValue.Closed)
    val scope = rememberCoroutineScope()
    val minimumRowHeight = with(LocalDensity.current) { 48.dp.toPx() }
    LaunchedEffect(state.session?.ownerId, state.connection) {
        val session = state.session
        if (session == null) sheet = null
        if (
            session != null &&
                state.connection == Connection.ONLINE &&
                promptedOwner != session.ownerId
        ) {
            promptedOwner = session.ownerId
            if (session.grant == null) sheet = Sheet.PERMISSION
        }
    }
    BackHandler(enabled = screen != Screen.CHAT) { screen = Screen.CHAT }
    MaterialTheme {
        Surface(modifier = Modifier.fillMaxSize(), color = MaterialTheme.colorScheme.surface) {
            if (state.session == null) {
                ConnectScreen(state, model)
            } else {
                ModalNavigationDrawer(
                    drawerState = drawer,
                    gesturesEnabled = screen == Screen.CHAT,
                    drawerContent = {
                        ModalDrawerSheet(
                            drawerShape = RoundedCornerShape(topEnd = 20.dp, bottomEnd = 20.dp)
                        ) {
                            Text(
                                stringResource(R.string.conversations),
                                style = MaterialTheme.typography.titleLarge,
                                modifier = Modifier.padding(24.dp),
                            )
                            NavigationDrawerItem(
                                label = { Text(stringResource(R.string.new_conversation)) },
                                icon = { AppIcon(R.drawable.ic_plus) },
                                selected = state.selectedGoalId == null,
                                shape = RoundedCornerShape(10.dp),
                                onClick = {
                                    model.selectGoal(null)
                                    scope.launch { drawer.close() }
                                    screen = Screen.CHAT
                                },
                                modifier = Modifier.padding(horizontal = 12.dp),
                            )
                            LazyColumn(
                                modifier =
                                    Modifier.weight(1f).onSizeChanged { size ->
                                        if (size.height > 0)
                                            model.viewportCapacity(
                                                kotlin.math
                                                    .ceil(size.height / minimumRowHeight.toDouble())
                                                    .toLong()
                                                    .toULong()
                                            )
                                    }
                            ) {
                                if (state.goals.isEmpty())
                                    item {
                                        Text(
                                            stringResource(R.string.no_conversations),
                                            style = MaterialTheme.typography.bodySmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                            modifier = Modifier.padding(24.dp),
                                        )
                                    }
                                items(state.goals, key = { it.goalId }) { goal ->
                                    NavigationDrawerItem(
                                        label = {
                                            Text(
                                                goal.objective,
                                                maxLines = 2,
                                                overflow = TextOverflow.Ellipsis,
                                            )
                                        },
                                        selected = goal.goalId == state.selectedGoalId,
                                        shape = RoundedCornerShape(10.dp),
                                        onClick = {
                                            model.selectGoal(goal.goalId)
                                            scope.launch { drawer.close() }
                                            screen = Screen.CHAT
                                        },
                                        modifier = Modifier.padding(horizontal = 12.dp),
                                    )
                                }
                                if (state.loadingGoals)
                                    item {
                                        CircularProgressIndicator(
                                            Modifier.padding(24.dp).size(24.dp),
                                            strokeWidth = 2.dp,
                                        )
                                    }
                                else if (state.nextCursor != null)
                                    item {
                                        TextButton(
                                            onClick = { model.loadMore() },
                                            modifier = Modifier.padding(horizontal = 12.dp),
                                        ) {
                                            Text(stringResource(R.string.load_more))
                                        }
                                    }
                            }
                            NavigationDrawerItem(
                                label = { Text(stringResource(R.string.my_agent)) },
                                icon = { AppIcon(R.drawable.ic_gesang) },
                                selected = screen == Screen.PROFILE,
                                shape = RoundedCornerShape(10.dp),
                                onClick = {
                                    screen = Screen.PROFILE
                                    scope.launch { drawer.close() }
                                },
                                modifier = Modifier.padding(12.dp),
                            )
                        }
                    },
                ) {
                    when (screen) {
                        Screen.CHAT ->
                            ChatScreen(
                                state = state,
                                model = model,
                                onDrawer = { scope.launch { drawer.open() } },
                                onProfile = { screen = Screen.PROFILE },
                                onSheet = { sheet = it },
                            )
                        Screen.PROFILE ->
                            ProfileScreen(
                                state,
                                { screen = Screen.CHAT },
                                {
                                    screen = Screen.LEARNING
                                    model.loadLearning()
                                },
                            ) {
                                sheet = it
                            }
                        Screen.LEARNING -> LearningScreen(state, model) { screen = Screen.PROFILE }
                    }
                }
            }
            when (sheet) {
                Sheet.PERMISSION -> PermissionSheet(state, model) { sheet = null }
                Sheet.ACTIVITY -> DetailsSheet(state.goal) { sheet = null }
                Sheet.CONNECTION -> ConnectionSheet(state, model) { sheet = null }
                Sheet.NOTIFICATIONS -> NotificationsSheet(state, model) { sheet = null }
                Sheet.STOP ->
                    ConfirmDialog(
                        R.string.stop_title,
                        R.string.stop_body,
                        R.string.stop_action,
                        onDismiss = { sheet = null },
                        onConfirm = {
                            sheet = null
                            model.stopGoal()
                        },
                    )
                Sheet.SIGN_OUT ->
                    ConfirmDialog(
                        R.string.sign_out,
                        R.string.sign_out_body,
                        R.string.sign_out,
                        onDismiss = { sheet = null },
                        onConfirm = {
                            sheet = null
                            screen = Screen.CHAT
                            model.signOut()
                        },
                    )
                Sheet.REVOKE ->
                    ConfirmDialog(
                        R.string.revoke_action,
                        R.string.revoke_body,
                        R.string.revoke_action,
                        onDismiss = { sheet = null },
                        onConfirm = {
                            sheet = null
                            model.revoke()
                        },
                    )
                Sheet.RESUME -> {
                    val message = stringResource(R.string.resume_message)
                    AlertDialog(
                        onDismissRequest = { sheet = null },
                        shape = RoundedCornerShape(24.dp),
                        title = { Text(stringResource(R.string.resume_task)) },
                        text = {
                            Text(
                                stringResource(
                                    R.string.resume_body,
                                    stringResource(
                                        state.session?.grant?.permission?.label()
                                            ?: R.string.permission_not_selected
                                    ),
                                )
                            )
                        },
                        confirmButton = {
                            TextButton(
                                onClick = {
                                    sheet = null
                                    model.resumeGoal(message)
                                },
                                enabled = state.session?.grant != null,
                            ) {
                                Text(stringResource(R.string.resume_task))
                            }
                        },
                        dismissButton = {
                            TextButton(onClick = { sheet = null }) {
                                Text(stringResource(R.string.cancel))
                            }
                        },
                    )
                }
                null -> Unit
            }
        }
    }
}

@Composable
private fun ConnectScreen(state: AppState, model: AppViewModel) {
    val keyboard = LocalSoftwareKeyboardController.current
    BoxWithConstraints(Modifier.fillMaxSize().safeDrawingPadding().imePadding()) {
        Column(
            modifier =
                Modifier.fillMaxWidth()
                    .verticalScroll(rememberScrollState())
                    .heightIn(min = maxHeight)
                    .padding(horizontal = 24.dp, vertical = 24.dp),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.Start,
        ) {
            AppIcon(R.drawable.ic_gesang, modifier = Modifier.size(44.dp))
            Spacer(Modifier.height(28.dp))
            Text(
                stringResource(R.string.connect_title),
                style = MaterialTheme.typography.headlineMedium,
            )
            Spacer(Modifier.height(12.dp))
            Text(
                stringResource(R.string.connect_body),
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(Modifier.height(32.dp))
            OutlinedTextField(
                value = state.endpoint,
                onValueChange = model::editEndpoint,
                modifier = Modifier.fillMaxWidth(),
                label = { Text(stringResource(R.string.server_address)) },
                placeholder = { Text(stringResource(R.string.server_hint)) },
                keyboardOptions =
                    KeyboardOptions(keyboardType = KeyboardType.Uri, imeAction = ImeAction.Go),
                keyboardActions =
                    KeyboardActions(
                        onGo = {
                            if (!state.busy) {
                                keyboard?.hide()
                                model.beginSignIn()
                            }
                        }
                    ),
                singleLine = true,
                enabled = !state.busy,
                shape = RoundedCornerShape(12.dp),
            )
            state.error?.let { InlineError(it) }
            Spacer(Modifier.height(24.dp))
            Button(
                onClick = {
                    keyboard?.hide()
                    model.beginSignIn()
                },
                enabled = state.endpoint.isNotBlank() && !state.busy,
                modifier = Modifier.heightIn(min = 48.dp),
            ) {
                if (state.busy)
                    CircularProgressIndicator(
                        Modifier.size(18.dp),
                        color = MaterialTheme.colorScheme.onPrimary,
                        strokeWidth = 2.dp,
                    )
                else Text(stringResource(R.string.connect_action))
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ChatScreen(
    state: AppState,
    model: AppViewModel,
    onDrawer: () -> Unit,
    onProfile: () -> Unit,
    onSheet: (Sheet) -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    var replaceDraft by remember { mutableStateOf(false) }
    val goal = state.goal
    val messages = rememberLazyListState()
    var autoFollow by remember(state.selectedGoalId) { mutableStateOf(true) }
    val dragged by messages.interactionSource.collectIsDraggedAsState()
    var historyRows by remember { mutableStateOf(1uL) }
    val rowPixels = with(LocalDensity.current) { 48.dp.toPx() }
    LaunchedEffect(dragged, messages) {
        snapshotFlow {
            (messages.layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: 0) >=
                messages.layoutInfo.totalItemsCount - 2
        }
            .collect { atBottom -> if (dragged) autoFollow = atBottom }
    }
    LaunchedEffect(
        state.selectedGoalId,
        state.transcript.lastOrNull()?.entryId,
        state.live?.generation?.content?.lastOrNull()?.text,
        goal?.version,
    ) {
        if (autoFollow && state.selectedGoalId != null) {
            withFrameNanos {}
            messages.scrollToItem((messages.layoutInfo.totalItemsCount - 1).coerceAtLeast(0))
        }
    }
    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Column {
                        Text(
                            goal?.objective ?: stringResource(R.string.app_name),
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            style = MaterialTheme.typography.titleMedium,
                        )
                        if (goal != null)
                            Text(
                                stringResource(
                                    if (
                                        state.connection == Connection.ONLINE &&
                                            state.observation == ObservationConnection.LIVE
                                    )
                                        goal.phase.label()
                                    else if (
                                        state.connection == Connection.CONNECTING ||
                                            state.observation == ObservationConnection.CONNECTING
                                    )
                                        R.string.connection_syncing
                                    else R.string.connection_offline
                                ),
                                style = MaterialTheme.typography.bodySmall,
                                color =
                                    if (
                                        state.connection == Connection.ONLINE &&
                                            state.observation == ObservationConnection.LIVE
                                    )
                                        phaseColor(goal.phase)
                                    else MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                    }
                },
                navigationIcon = {
                    IconButton(onClick = onDrawer) {
                        AppIcon(
                            R.drawable.ic_menu,
                            description = stringResource(R.string.conversations),
                        )
                    }
                },
                actions = {
                    IconButton(onClick = { menu = true }) {
                        AppIcon(
                            R.drawable.ic_ellipsis_vertical,
                            description = stringResource(R.string.more_options),
                        )
                    }
                    DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                        DropdownMenuItem(
                            text = { Text(stringResource(R.string.my_agent)) },
                            onClick = {
                                menu = false
                                onProfile()
                            },
                        )
                        DropdownMenuItem(
                            text = { Text(stringResource(R.string.refresh)) },
                            onClick = {
                                menu = false
                                model.refresh()
                            },
                        )
                        if (
                            goal != null &&
                                goal.phase !in
                                    setOf(
                                        GoalPhase.COMPLETED,
                                        GoalPhase.STOPPED,
                                        GoalPhase.FAILED,
                                        GoalPhase.STOPPING,
                                    )
                        ) {
                            DropdownMenuItem(
                                text = { Text(stringResource(R.string.stop_action)) },
                                onClick = {
                                    menu = false
                                    onSheet(Sheet.STOP)
                                },
                            )
                        }
                    }
                },
                colors =
                    TopAppBarDefaults.topAppBarColors(
                        containerColor = MaterialTheme.colorScheme.surface
                    ),
            )
        },
        bottomBar = {
            Composer(state, model::editDraft) {
                autoFollow = true
                model.send()
            }
        },
        containerColor = MaterialTheme.colorScheme.surface,
    ) { padding ->
        LazyColumn(
            state = messages,
            contentPadding = PaddingValues(start = 24.dp, end = 24.dp, top = 20.dp, bottom = 24.dp),
            modifier =
                Modifier.fillMaxSize().padding(padding).onSizeChanged { size ->
                    if (size.height > 0)
                        historyRows =
                            kotlin.math.ceil(size.height / rowPixels.toDouble()).toLong().toULong()
                },
            verticalArrangement = Arrangement.spacedBy(24.dp),
        ) {
            if (state.connection != Connection.ONLINE)
                item("connection") {
                    Column {
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                        ) {
                            AppIcon(R.drawable.ic_wifi_off)
                            Text(
                                stringResource(
                                    when (state.connection) {
                                        Connection.SIGNED_OUT -> R.string.error_authentication
                                        Connection.CONNECTING -> R.string.connection_syncing
                                        else -> R.string.connection_offline
                                    }
                                ),
                                style = MaterialTheme.typography.titleSmall,
                            )
                        }
                        Text(
                            stringResource(R.string.offline_body),
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                        goal?.synchronizedAtMs?.let {
                            Text(
                                stringResource(
                                    R.string.last_synced,
                                    DateUtils.getRelativeTimeSpanString(it),
                                ),
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                        TextButton(
                            onClick =
                                if (state.connection == Connection.SIGNED_OUT) model::beginSignIn
                                else model::refresh
                        ) {
                            Text(
                                stringResource(
                                    if (state.connection == Connection.SIGNED_OUT)
                                        R.string.sign_in_action
                                    else R.string.retry
                                )
                            )
                        }
                    }
                }
            state.error?.let { error -> item("error") { InlineError(error) } }
            if (state.loadingGoal)
                item("loading-goal") {
                    CircularProgressIndicator(Modifier.size(24.dp), strokeWidth = 2.dp)
                }
            if (goal == null && state.selectedGoalId == null && !state.hasPending)
                item("empty") {
                    Column(modifier = Modifier.padding(top = 80.dp, bottom = 40.dp)) {
                        Text(
                            stringResource(R.string.empty_title),
                            style = MaterialTheme.typography.headlineMedium,
                        )
                        Spacer(Modifier.height(12.dp))
                        Text(
                            stringResource(R.string.empty_body),
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                        if (state.session?.grant == null) {
                            Spacer(Modifier.height(24.dp))
                            TextButton(onClick = { onSheet(Sheet.PERMISSION) }) {
                                Text(stringResource(R.string.permission_title))
                            }
                        }
                    }
                }
            if (goal != null) {
                if (state.observation != ObservationConnection.LIVE)
                    item("stream-connection") {
                        Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                            Text(
                                stringResource(
                                    if (state.observation == ObservationConnection.CONNECTING)
                                        R.string.connection_syncing
                                    else R.string.transcript_reconnecting
                                ),
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            state.transcriptSynchronizedAtMs?.let {
                                Text(
                                    stringResource(
                                        R.string.last_synced,
                                        DateUtils.getRelativeTimeSpanString(it),
                                    ),
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                            state.observationError?.let { InlineError(it) }
                        }
                    }
                if (state.historyBefore != null || state.historyCursor != null)
                    item("older") {
                        TextButton(
                            onClick = { model.loadOlderMessages(historyRows) },
                            enabled = !state.loadingHistory,
                        ) {
                            Text(
                                stringResource(
                                    if (state.loadingHistory) R.string.loading
                                    else R.string.older_messages
                                )
                            )
                        }
                    }
                items(
                    state.transcript,
                    key = { "entry:${it.entryId}" },
                    contentType = { "entry" },
                ) { entry ->
                    TranscriptEntry(entry)
                }
                state.live?.let { live -> item("live") { LiveProgress(live) } }
                item("agent:${goal.goalId}") {
                    Column(verticalArrangement = Arrangement.spacedBy(18.dp)) {
                        if (state.transcript.isEmpty() && state.live?.generation == null)
                            Row(
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(10.dp),
                            ) {
                                AppIcon(R.drawable.ic_gesang, modifier = Modifier.size(24.dp))
                                Text(
                                    stringResource(R.string.app_name),
                                    style = MaterialTheme.typography.titleSmall,
                                )
                            }
                        if (state.transcript.isEmpty())
                            goal.resultSummary?.let { MessageMarkdown(it) }
                        if (goal.resultSummary == null) {
                            Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                                AppIcon(
                                    R.drawable.ic_clock_3,
                                    modifier = Modifier.padding(top = 3.dp),
                                    tint = phaseColor(goal.phase),
                                )
                                Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                                    Text(
                                        stringResource(goal.phase.label()),
                                        style = MaterialTheme.typography.titleSmall,
                                    )
                                    goal.waitingReason?.let {
                                        Text(
                                            it,
                                            style = MaterialTheme.typography.bodyLarge,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                        )
                                    }
                                    if (goal.phase == GoalPhase.WAITING_USER)
                                        Text(
                                            stringResource(R.string.question_reply),
                                            style = MaterialTheme.typography.bodySmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                        )
                                    if (goal.phase == GoalPhase.WAITING_AUTHORIZATION)
                                        TextButton(
                                            onClick = {
                                                onSheet(
                                                    if (state.session?.grant == null)
                                                        Sheet.PERMISSION
                                                    else Sheet.RESUME
                                                )
                                            },
                                            enabled = !state.busy && !state.hasPending,
                                        ) {
                                            Text(
                                                stringResource(
                                                    if (state.session?.grant == null)
                                                        R.string.permission_title
                                                    else R.string.resume_task
                                                )
                                            )
                                        }
                                }
                            }
                        }
                        TextButton(
                            onClick = { onSheet(Sheet.ACTIVITY) },
                            contentPadding = PaddingValues(0.dp),
                            modifier = Modifier.fillMaxWidth(),
                        ) {
                            Text(
                                stringResource(R.string.activity),
                                modifier = Modifier.weight(1f),
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            AppIcon(
                                R.drawable.ic_chevron_right,
                                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                    }
                }
            }
            if (state.hasPending)
                item("pending") {
                    Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
                        state.pendingText?.let {
                            Text(it, style = MaterialTheme.typography.bodyLarge)
                        }
                        Text(
                            stringResource(
                                if (state.pendingRejected) R.string.receipt_rejected
                                else R.string.pending_receipt
                            ),
                            style = MaterialTheme.typography.bodySmall,
                            color =
                                if (state.pendingRejected) MaterialTheme.colorScheme.error
                                else MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                        if (state.pendingRejected) {
                            if (state.pendingText != null)
                                TextButton(
                                    onClick = {
                                        if (
                                            state.draft.isNotBlank() &&
                                                state.draft != state.pendingText
                                        )
                                            replaceDraft = true
                                        else model.editRejectedDelivery()
                                    },
                                    enabled = !state.busy,
                                ) {
                                    Text(stringResource(R.string.return_to_draft))
                                }
                            TextButton(
                                onClick = model::discardRejectedDelivery,
                                enabled = !state.busy,
                            ) {
                                Text(stringResource(R.string.discard_unsent))
                            }
                        }
                    }
                }
        }
    }
    if (replaceDraft)
        ConfirmDialog(
            R.string.replace_draft_title,
            R.string.replace_draft_body,
            R.string.return_to_draft,
            onDismiss = { replaceDraft = false },
            onConfirm = {
                replaceDraft = false
                model.editRejectedDelivery()
            },
        )
}

@Composable
private fun Composer(state: AppState, onDraft: (String) -> Unit, onSend: () -> Unit) {
    val connected = state.connection == Connection.ONLINE
    val terminal = state.goal?.phase == GoalPhase.FAILED
    val needsGrant =
        state.goal == null ||
            state.goal?.phase == GoalPhase.COMPLETED ||
            state.goal?.phase == GoalPhase.STOPPED
    val messageLabel = stringResource(R.string.message_input)
    val sendLabel = stringResource(if (state.busy) R.string.sending else R.string.send)
    Row(
        verticalAlignment = Alignment.Bottom,
        horizontalArrangement = Arrangement.spacedBy(6.dp),
        modifier =
            Modifier.fillMaxWidth()
                .navigationBarsPadding()
                .imePadding()
                .padding(horizontal = 16.dp, vertical = 8.dp)
                .background(LocalSurfaceColors.current.soft, RoundedCornerShape(30.dp))
                .padding(6.dp),
    ) {
        BasicTextField(
            value = state.draft,
            onValueChange = onDraft,
            textStyle =
                MaterialTheme.typography.bodyLarge.copy(
                    color = MaterialTheme.colorScheme.onSurface
                ),
            cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
            maxLines = 5,
            readOnly = state.loadingDraft,
            modifier =
                Modifier.weight(1f)
                    .heightIn(min = 48.dp)
                    .padding(horizontal = 16.dp, vertical = 11.dp)
                    .semantics { contentDescription = messageLabel },
            decorationBox = { inner ->
                Box {
                    if (state.draft.isEmpty())
                        Text(
                            stringResource(
                                if (!connected) R.string.composer_offline
                                else if (state.goal == null) R.string.composer_new
                                else R.string.composer_reply
                            ),
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    inner()
                }
            },
        )
        FilledIconButton(
            onClick = onSend,
            enabled =
                connected &&
                    !terminal &&
                    (!needsGrant || state.session?.grant != null) &&
                    state.draft.isNotBlank() &&
                    !state.busy &&
                    !state.hasPending &&
                    !state.loadingDraft,
            modifier = Modifier.size(48.dp).semantics { contentDescription = sendLabel },
            shape = CircleShape,
        ) {
            if (state.busy)
                CircularProgressIndicator(
                    Modifier.size(20.dp),
                    strokeWidth = 2.dp,
                    color = MaterialTheme.colorScheme.onPrimary,
                )
            else AppIcon(R.drawable.ic_arrow_up)
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ProfileScreen(
    state: AppState,
    onBack: () -> Unit,
    onLearning: () -> Unit,
    onSheet: (Sheet) -> Unit,
) {
    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Text(
                        stringResource(R.string.my_agent),
                        style = MaterialTheme.typography.titleMedium,
                    )
                },
                navigationIcon = {
                    IconButton(onClick = onBack) {
                        AppIcon(
                            R.drawable.ic_arrow_left,
                            description = stringResource(R.string.back),
                        )
                    }
                },
            )
        },
        containerColor = MaterialTheme.colorScheme.surface,
    ) { padding ->
        LazyColumn(
            modifier = Modifier.padding(padding),
            contentPadding = PaddingValues(horizontal = 24.dp, vertical = 24.dp),
        ) {
            item {
                AppIcon(R.drawable.ic_gesang, modifier = Modifier.size(44.dp))
                Spacer(Modifier.height(16.dp))
                Text(
                    stringResource(R.string.my_agent_subtitle),
                    style = MaterialTheme.typography.headlineSmall,
                )
                Spacer(Modifier.height(32.dp))
            }
            item {
                SettingsRow(R.drawable.ic_cloud, R.string.cloud_connection, state.endpoint) {
                    onSheet(Sheet.CONNECTION)
                }
            }
            item { HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant) }
            item {
                SettingsRow(
                    R.drawable.ic_shield_check,
                    R.string.permission_title,
                    stringResource(
                        state.session?.grant?.permission?.label()
                            ?: R.string.permission_not_selected
                    ),
                ) {
                    onSheet(Sheet.PERMISSION)
                }
            }
            item {
                SettingsRow(
                    R.drawable.ic_bell,
                    R.string.notifications,
                    stringResource(R.string.notification_private),
                ) {
                    onSheet(Sheet.NOTIFICATIONS)
                }
            }
            item {
                SettingsRow(
                    R.drawable.ic_leaf,
                    R.string.improvement,
                    stringResource(R.string.improvement_body),
                    onLearning,
                )
            }
            item {
                Spacer(Modifier.height(24.dp))
                TextButton(
                    onClick = { onSheet(Sheet.REVOKE) },
                    enabled = state.session?.grant != null && !state.busy,
                ) {
                    Text(stringResource(R.string.revoke_action))
                }
            }
            item {
                TextButton(onClick = { onSheet(Sheet.SIGN_OUT) }) {
                    Text(
                        stringResource(R.string.sign_out),
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ConnectionSheet(state: AppState, model: AppViewModel, onDismiss: () -> Unit) {
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
    ) {
        Column(
            Modifier.padding(horizontal = 24.dp).padding(bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            SheetHeading(R.string.cloud_connection, onDismiss)
            SelectionContainer { Text(state.endpoint, style = MaterialTheme.typography.bodyLarge) }
            Text(
                stringResource(
                    when (state.connection) {
                        Connection.ONLINE -> R.string.connection_online
                        Connection.CONNECTING -> R.string.connection_syncing
                        Connection.OFFLINE -> R.string.connection_offline
                        Connection.SIGNED_OUT -> R.string.error_authentication
                    }
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Button(onClick = model::refresh, enabled = !state.busy) {
                Text(stringResource(R.string.refresh))
            }
            if (state.connection == Connection.SIGNED_OUT)
                TextButton(onClick = model::beginSignIn) {
                    Text(stringResource(R.string.sign_in_action))
                }
        }
    }
}

@Composable
private fun SettingsRow(
    @DrawableRes icon: Int,
    @StringRes title: Int,
    subtitle: String,
    onClick: () -> Unit,
) {
    Surface(onClick = onClick, color = MaterialTheme.colorScheme.surface) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(vertical = 18.dp),
            horizontalArrangement = Arrangement.spacedBy(16.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            AppIcon(icon, tint = MaterialTheme.colorScheme.onSurfaceVariant)
            Column(modifier = Modifier.weight(1f)) {
                Text(stringResource(title), style = MaterialTheme.typography.titleSmall)
                Text(
                    subtitle,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            AppIcon(R.drawable.ic_chevron_right, tint = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun PermissionSheet(state: AppState, model: AppViewModel, onDismiss: () -> Unit) {
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
        shape = RoundedCornerShape(topStart = 24.dp, topEnd = 24.dp),
    ) {
        Column(
            modifier =
                Modifier.fillMaxWidth()
                    .verticalScroll(rememberScrollState())
                    .padding(horizontal = 24.dp)
                    .padding(bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            SheetHeading(R.string.permission_title, onDismiss)
            Text(
                stringResource(R.string.permission_intro),
                style = MaterialTheme.typography.bodyLarge,
            )
            Column(Modifier.selectableGroup(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                for (permission in Permission.entries) {
                    val selected = permission == state.session?.grant?.permission
                    Row(
                        modifier =
                            Modifier.fillMaxWidth()
                                .background(
                                    if (selected) MaterialTheme.colorScheme.primaryContainer
                                    else Color.Transparent,
                                    RoundedCornerShape(16.dp),
                                )
                                .selectable(
                                    selected = selected,
                                    enabled = !state.busy,
                                    role = Role.RadioButton,
                                    onClick = { model.selectPermission(permission) },
                                )
                                .padding(horizontal = 12.dp, vertical = 16.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(12.dp),
                    ) {
                        RadioButton(selected = selected, onClick = null, enabled = !state.busy)
                        Column(Modifier.weight(1f)) {
                            Text(
                                stringResource(permission.label()),
                                style = MaterialTheme.typography.titleSmall,
                            )
                            Text(
                                stringResource(permission.description()),
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                    }
                }
            }
            if (state.session?.grant?.permission == Permission.SCOPED)
                Text(
                    stringResource(R.string.permission_scope_empty),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            state.error?.let { InlineError(it) }
            if (state.busy) CircularProgressIndicator(Modifier.size(24.dp), strokeWidth = 2.dp)
            Text(
                stringResource(R.string.permission_boundary),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun DetailsSheet(goal: GoalView?, onDismiss: () -> Unit) {
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
    ) {
        LazyColumn(
            contentPadding = PaddingValues(start = 24.dp, end = 24.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            item { SheetHeading(R.string.details, onDismiss) }
            if (goal != null) {
                item {
                    Text(
                        stringResource(goal.phase.label()),
                        style = MaterialTheme.typography.titleMedium,
                    )
                    Text(
                        stringResource(
                            R.string.last_synced,
                            DateUtils.getRelativeTimeSpanString(goal.synchronizedAtMs),
                        ),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                if (goal.completionCriteria.isNotEmpty()) {
                    item {
                        Text(
                            stringResource(R.string.completion_criteria),
                            style = MaterialTheme.typography.titleSmall,
                        )
                    }
                    items(goal.completionCriteria) { criterion ->
                        Text(criterion, style = MaterialTheme.typography.bodyLarge)
                    }
                }
                if (goal.evidence.isNotEmpty()) {
                    item {
                        Text(
                            stringResource(R.string.verified_results),
                            style = MaterialTheme.typography.titleSmall,
                        )
                    }
                    items(goal.evidence) { evidence ->
                        Column {
                            goal.completionCriteria.getOrNull(evidence.criterion.toInt())?.let {
                                Text(it, style = MaterialTheme.typography.bodyLarge)
                            }
                            evidence.artifactPath?.let {
                                Text(
                                    it,
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun NotificationsSheet(state: AppState, model: AppViewModel, onDismiss: () -> Unit) {
    val context = LocalContext.current
    val permission =
        rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
            if (granted) model.enableNotifications() else model.refreshNotifications()
        }
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
    ) {
        Column(
            Modifier.padding(horizontal = 24.dp).padding(bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            SheetHeading(R.string.notifications, onDismiss)
            Text(
                stringResource(R.string.notification_body),
                style = MaterialTheme.typography.bodyLarge,
            )
            if (BuildConfig.MESSAGING_CONFIGURED)
                Text(
                    stringResource(R.string.notification_private),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            Text(
                stringResource(
                    when (state.pushPhase) {
                        PushPhase.UNAVAILABLE -> R.string.notification_unconfigured
                        PushPhase.OFF -> R.string.notification_off
                        PushPhase.PERMISSION_REQUIRED -> R.string.notification_permission_required
                        PushPhase.REGISTERING -> R.string.notification_registering
                        PushPhase.SERVER_PENDING -> R.string.notification_server_pending
                        PushPhase.REGISTERED -> R.string.notification_registered
                        PushPhase.REVOCATION_PENDING -> R.string.notification_revocation_pending
                    }
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            state.pushError?.let { InlineError(it) }
            if (BuildConfig.MESSAGING_CONFIGURED) {
                if (
                    state.pushPhase == PushPhase.OFF ||
                        state.pushPhase == PushPhase.PERMISSION_REQUIRED ||
                        state.pushPhase == PushPhase.SERVER_PENDING
                ) {
                    Button(
                        onClick = {
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU)
                                permission.launch(Manifest.permission.POST_NOTIFICATIONS)
                            else model.enableNotifications()
                        }
                    ) {
                        Text(
                            stringResource(
                                if (state.pushPhase == PushPhase.SERVER_PENDING) R.string.retry
                                else R.string.notification_enable
                            )
                        )
                    }
                }
                if (
                    state.pushPhase == PushPhase.REGISTERED ||
                        state.pushPhase == PushPhase.REGISTERING ||
                        state.pushPhase == PushPhase.SERVER_PENDING ||
                        state.pushPhase == PushPhase.REVOCATION_PENDING
                ) {
                    TextButton(onClick = model::disableNotifications) {
                        Text(
                            stringResource(
                                if (state.pushPhase == PushPhase.REVOCATION_PENDING)
                                    R.string.notification_retry_revoke
                                else R.string.notification_disable
                            )
                        )
                    }
                }
            }
            TextButton(
                onClick = {
                    val intent =
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                            Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS)
                                .putExtra(Settings.EXTRA_APP_PACKAGE, context.packageName)
                        } else
                            Intent(
                                Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                                Uri.fromParts("package", context.packageName, null),
                            )
                    context.startActivity(intent)
                }
            ) {
                Text(stringResource(R.string.notification_open_settings))
            }
        }
    }
}

@Composable
private fun SheetHeading(@StringRes title: Int, onDismiss: () -> Unit) {
    Row(modifier = Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
        Text(
            stringResource(title),
            style = MaterialTheme.typography.titleLarge,
            modifier = Modifier.weight(1f).semantics { heading() },
        )
        IconButton(onClick = onDismiss) {
            AppIcon(R.drawable.ic_x, description = stringResource(R.string.close))
        }
    }
}

@Composable
private fun ConfirmDialog(
    @StringRes title: Int,
    @StringRes body: Int,
    @StringRes action: Int,
    onDismiss: () -> Unit,
    onConfirm: () -> Unit,
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(title)) },
        text = { Text(stringResource(body)) },
        confirmButton = { TextButton(onClick = onConfirm) { Text(stringResource(action)) } },
        dismissButton = {
            TextButton(onClick = onDismiss) { Text(stringResource(R.string.cancel)) }
        },
        shape = RoundedCornerShape(24.dp),
    )
}

@Composable
private fun InlineError(@StringRes message: Int) {
    Text(
        stringResource(message),
        style = MaterialTheme.typography.bodySmall,
        color = MaterialTheme.colorScheme.error,
        modifier =
            Modifier.padding(vertical = 8.dp).semantics { liveRegion = LiveRegionMode.Polite },
    )
}

@Composable
internal fun AppIcon(
    @DrawableRes drawable: Int,
    modifier: Modifier = Modifier,
    description: String? = null,
    tint: Color = MaterialTheme.colorScheme.onSurface,
) {
    Icon(
        painterResource(drawable),
        contentDescription = description,
        modifier = modifier.size(22.dp),
        tint = tint,
    )
}

@StringRes
private fun Permission.label(): Int =
    when (this) {
        Permission.FULL_ACCESS -> R.string.permission_full
        Permission.SCOPED -> R.string.permission_scoped
        Permission.ASK -> R.string.permission_ask
    }

@StringRes
private fun Permission.description(): Int =
    when (this) {
        Permission.FULL_ACCESS -> R.string.permission_full_body
        Permission.SCOPED -> R.string.permission_scoped_body
        Permission.ASK -> R.string.permission_ask_body
    }

@StringRes
private fun GoalPhase.label(): Int =
    when (this) {
        GoalPhase.ACCEPTED -> R.string.goal_accepted
        GoalPhase.RUNNING -> R.string.goal_running
        GoalPhase.WAITING_USER -> R.string.goal_waiting_user
        GoalPhase.WAITING_AUTHORIZATION -> R.string.goal_waiting_authorization
        GoalPhase.WAITING_TIME -> R.string.goal_waiting_time
        GoalPhase.WAITING_DEPENDENCY -> R.string.goal_waiting_dependency
        GoalPhase.STOPPING -> R.string.goal_stopping
        GoalPhase.COMPLETED -> R.string.goal_completed
        GoalPhase.STOPPED -> R.string.goal_stopped
        GoalPhase.FAILED -> R.string.goal_failed
    }

@Composable
private fun phaseColor(phase: GoalPhase): Color =
    when (phase) {
        GoalPhase.RUNNING,
        GoalPhase.COMPLETED -> LocalSurfaceColors.current.success
        GoalPhase.WAITING_USER,
        GoalPhase.WAITING_AUTHORIZATION,
        GoalPhase.FAILED -> LocalSurfaceColors.current.warning
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }
