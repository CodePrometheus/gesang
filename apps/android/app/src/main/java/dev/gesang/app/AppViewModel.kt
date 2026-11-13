package dev.gesang.app

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import dev.gesang.app.platform.Account
import dev.gesang.app.platform.DraftWriter
import dev.gesang.app.platform.MessagingEvents
import dev.gesang.app.platform.MessagingStore
import dev.gesang.app.platform.PushPhase
import dev.gesang.app.platform.SecureStore
import dev.gesang.app.platform.StoredDelivery
import dev.gesang.app.platform.StoredDraft
import dev.gesang.app.platform.TranscriptStore
import dev.gesang.app.platform.TranscriptWriter
import dev.gesang.app.platform.accountRecordKey
import dev.gesang.nativeclient.AuthorizationAttempt
import dev.gesang.nativeclient.Client
import dev.gesang.nativeclient.ClientException
import dev.gesang.nativeclient.GoalPhase
import dev.gesang.nativeclient.GoalView
import dev.gesang.nativeclient.LearningAction
import dev.gesang.nativeclient.LearningSettingsChange
import dev.gesang.nativeclient.LearningView
import dev.gesang.nativeclient.LiveView
import dev.gesang.nativeclient.PendingCommand
import dev.gesang.nativeclient.Permission
import dev.gesang.nativeclient.ReceiptState
import dev.gesang.nativeclient.SessionView
import dev.gesang.nativeclient.TranscriptCacheMetadata
import dev.gesang.nativeclient.TranscriptEntryView
import java.util.UUID
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

internal enum class Connection {
    CONNECTING,
    ONLINE,
    OFFLINE,
    SIGNED_OUT,
}

internal enum class ObservationConnection {
    CONNECTING,
    LIVE,
    OFFLINE,
}

private data class NotificationDestination(val agentId: String, val goalId: String)

internal data class AppState(
    val endpoint: String = "",
    val session: SessionView? = null,
    val connection: Connection = Connection.SIGNED_OUT,
    val goals: List<GoalView> = emptyList(),
    val selectedGoalId: String? = null,
    val draft: String = "",
    val draftRevision: String = UUID.randomUUID().toString(),
    val loadingDraft: Boolean = false,
    val loadingGoal: Boolean = false,
    val busy: Boolean = false,
    val hasPending: Boolean = false,
    val error: Int? = null,
    val browserDestination: String? = null,
    val nextCursor: String? = null,
    val loadingGoals: Boolean = false,
    val pendingText: String? = null,
    val pendingRejected: Boolean = false,
    val transcript: List<TranscriptEntryView> = emptyList(),
    val live: LiveView? = null,
    val observation: ObservationConnection = ObservationConnection.CONNECTING,
    val observationError: Int? = null,
    val transcriptSynchronizedAtMs: Long? = null,
    val historyBefore: ULong? = null,
    val historyCursor: String? = null,
    val historyStarted: Boolean = false,
    val loadingHistory: Boolean = false,
    val learning: LearningView? = null,
    val loadingLearning: Boolean = false,
    val learningBusy: Boolean = false,
    val learningError: Int? = null,
    val pushPhase: PushPhase =
        if (BuildConfig.MESSAGING_CONFIGURED) PushPhase.OFF else PushPhase.UNAVAILABLE,
    val pushError: Int? = null,
) {
    val goal: GoalView?
        get() = goals.firstOrNull { it.goalId == selectedGoalId }
}

/** Device lifecycle owns observation and drafts, never the lifetime of cloud work. */
internal class AppViewModel(application: Application) : AndroidViewModel(application) {
    private val stateFlow =
        MutableStateFlow(AppState(busy = true, connection = Connection.CONNECTING))
    val state = stateFlow.asStateFlow()
    private val store = SecureStore(application)
    private var account: Account? = null
    private var bootstrap: Job? = null
    private var client: Client? = null
    private var attempt: AuthorizationAttempt? = null
    private var attemptEndpoint: String? = null
    private var delivery: StoredDelivery? = null
    private var synchronization: Job? = null
    private var focus: Job? = null
    private var authorizationJob: Job? = null
    private var goalObservation: Job? = null
    private var conversationObservation: Job? = null
    private var pushSynchronization: Job? = null
    private var pushDirty = false
    private var draftFocusKey: String? = null
    private var goalConnected = false
    private var conversationConnected = false
    private var goalObservationFailed = false
    private var conversationObservationFailed = false
    private var accountJob = SupervisorJob(viewModelScope.coroutineContext[Job])
    private var accountScope = CoroutineScope(viewModelScope.coroutineContext + accountJob)
    private var signingOut = false
    private var foreground = true
    private var exchangingAuthorization = false
    private var notificationDestination: NotificationDestination? = null
    private var pageLimit: ULong? = null
    private val draftWriter =
        DraftWriter(viewModelScope, store) {
            stateFlow.update { it.copy(error = R.string.error_storage) }
        }
    private val messaging = MessagingStore(application, store)
    private val transcriptStore = TranscriptStore(application, store)
    private val transcriptWriter =
        TranscriptWriter(viewModelScope, transcriptStore) {
            stateFlow.update { it.copy(error = R.string.error_storage) }
        }

    init {
        viewModelScope.launch { MessagingEvents.changes.collect { synchronizePush() } }
        bootstrap = viewModelScope.launch {
            try {
                val pendingAuthorization = withContext(Dispatchers.IO) { store.authorization() }
                pendingAuthorization?.let {
                    try {
                        attempt =
                            Client(it.endpoint, null, BuildConfig.DEBUG)
                                .restoreAuthorization(it.encoded)
                        attemptEndpoint = it.endpoint
                    } finally {
                        it.encoded.fill(0)
                    }
                }
                account = withContext(Dispatchers.IO) { store.account() }
                account?.let {
                    client = Client(it.endpoint, it.tokens.accessToken, BuildConfig.DEBUG)
                    stateFlow.update { state ->
                        state.copy(endpoint = it.endpoint, connection = Connection.CONNECTING)
                    }
                    if (it.ownerId != null && it.agentId != null && it.synchronizedAtMs != null) {
                        val cachedSession =
                            SessionView(it.ownerId, it.agentId, null, it.synchronizedAtMs)
                        stateFlow.update { state -> state.copy(session = cachedSession) }
                        client?.let { current -> restoreLocalState(current, cachedSession) }
                        selectGoal(state.value.selectedGoalId)
                    }
                    refresh()
                }
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
            } finally {
                stateFlow.update {
                    it.copy(
                        busy = false,
                        connection = if (account == null) Connection.SIGNED_OUT else it.connection,
                    )
                }
            }
        }
    }

    fun editEndpoint(value: String) {
        stateFlow.update { it.copy(endpoint = value, error = null) }
    }

    fun beginSignIn() {
        if (state.value.busy || signingOut) return
        authorizationJob = viewModelScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                val endpoint = state.value.endpoint.trim()
                val candidate = Client(endpoint, null, BuildConfig.DEBUG)
                val authorization = candidate.beginAuthorization()
                val encoded = authorization.encode()
                try {
                    withContext(Dispatchers.IO) { store.saveAuthorization(endpoint, encoded) }
                } finally {
                    encoded.fill(0)
                }
                attempt = authorization
                attemptEndpoint = endpoint
                stateFlow.update { it.copy(browserDestination = authorization.authorizationUrl()) }
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun browserOpened() {
        stateFlow.update { it.copy(browserDestination = null) }
    }

    fun browserUnavailable() {
        stateFlow.update { it.copy(error = R.string.error_browser_unavailable) }
    }

    fun completeSignIn(callback: String) {
        if (signingOut || exchangingAuthorization) return
        exchangingAuthorization = true
        authorizationJob =
            viewModelScope
                .launch {
                    bootstrap?.join()
                    val authorization = attempt
                    val endpoint = attemptEndpoint
                    if (authorization == null || endpoint == null) {
                        stateFlow.update { it.copy(error = R.string.error_authorization_lost) }
                        return@launch
                    }
                    stateFlow.update { it.copy(busy = true, error = null) }
                    try {
                        authorization.validateCallback(callback)
                        withContext(Dispatchers.IO) { store.clear("authorization") }
                        attempt = null
                        attemptEndpoint = null
                        val tokens = authorization.complete(callback)
                        val saved = Account(endpoint, tokens, authorization.configuration())
                        accountJob.cancelAndJoin()
                        withContext(Dispatchers.IO) { store.saveAccount(saved) }
                        account = saved
                        accountJob = SupervisorJob(viewModelScope.coroutineContext[Job])
                        accountScope = CoroutineScope(viewModelScope.coroutineContext + accountJob)
                        client = Client(endpoint, tokens.accessToken, BuildConfig.DEBUG)
                        delivery = null
                        stateFlow.value =
                            AppState(
                                endpoint = endpoint,
                                busy = true,
                                connection = Connection.CONNECTING,
                            )
                        refresh()
                    } catch (failure: Exception) {
                        report(failure)
                    } finally {
                        stateFlow.update { it.copy(busy = false) }
                    }
                }
                .also { job -> job.invokeOnCompletion { exchangingAuthorization = false } }
    }

    fun refresh() {
        if (synchronization?.isActive == true || signingOut) return
        synchronization = accountScope.launch {
            val current = client ?: return@launch
            stateFlow.update { it.copy(connection = Connection.CONNECTING, error = null) }
            try {
                val session =
                    try {
                        current.session()
                    } catch (failure: ClientException.AuthenticationRequired) {
                        refreshCredentials()
                        client?.session() ?: throw failure
                    }
                val activeClient = client ?: return@launch
                val previousOwner = account?.ownerId
                if (previousOwner != null && previousOwner != session.ownerId) {
                    throw ClientException.AuthenticationRequired()
                }
                account =
                    account?.copy(
                        ownerId = session.ownerId,
                        agentId = session.agentId,
                        synchronizedAtMs = session.synchronizedAtMs,
                    )
                withContext(Dispatchers.IO) { account?.let(store::saveAccount) }
                if (previousOwner != session.ownerId) {
                    stateFlow.update {
                        it.copy(goals = emptyList(), draft = "", selectedGoalId = null)
                    }
                    delivery = null
                }
                stateFlow.update { it.copy(session = session) }
                restoreLocalState(activeClient, session)
                if (previousOwner != session.ownerId) selectGoal(null)
                stateFlow.update { it.copy(connection = Connection.ONLINE, error = null) }
                pageLimit?.let { loadPage(activeClient, session, it, null, replace = true) }
                persistGoals(activeClient, session)
                reconcilePending()
                openNotificationDestination(session)
                state.value.selectedGoalId?.let(::selectGoal)
                refreshNotifications()
            } catch (failure: Exception) {
                report(failure)
            }
        }
    }

    fun viewportCapacity(rows: ULong) {
        if (rows == 0uL || rows == pageLimit) return
        pageLimit = rows
        if (state.value.session != null && state.value.connection == Connection.ONLINE)
            loadMore(replace = true)
    }

    fun loadMore(replace: Boolean = false) {
        val current = client ?: return
        val session = state.value.session ?: return
        val limit = pageLimit ?: return
        if (state.value.loadingGoals) return
        val cursor = if (replace) null else state.value.nextCursor ?: return
        accountScope.launch {
            try {
                loadPage(current, session, limit, cursor, replace)
            } catch (failure: Exception) {
                report(failure)
            }
        }
    }

    private suspend fun loadPage(
        current: Client,
        session: SessionView,
        limit: ULong,
        start: String?,
        replace: Boolean,
    ) {
        stateFlow.update { it.copy(loadingGoals = true) }
        try {
            var cursor = start
            val seen = mutableSetOf<String>()
            start?.let(seen::add)
            var page: dev.gesang.nativeclient.GoalPageView
            do {
                page = current.goals(session.agentId, limit, cursor)
                cursor = page.cursor
                if (cursor != null && !seen.add(cursor)) error("The source cursor did not advance.")
            } while (page.goals.isEmpty() && cursor != null)
            stateFlow.update { old ->
                val retained =
                    if (replace) old.goals.filter { it.goalId == old.selectedGoalId } else old.goals
                val received = page.goals.mapTo(mutableSetOf()) { it.goalId }
                old.copy(
                    goals = retained.filterNot { it.goalId in received } + page.goals,
                    nextCursor = cursor,
                )
            }
            persistGoals(current, session)
        } finally {
            stateFlow.update { it.copy(loadingGoals = false) }
        }
    }

    private suspend fun refreshCredentials() {
        val saved = account ?: return
        val refresh = saved.tokens.refreshToken ?: return
        val current = client ?: return
        val tokens = current.refreshTokens(saved.configuration, refresh)
        val updated = saved.copy(tokens = tokens)
        withContext(Dispatchers.IO) { store.saveAccount(updated) }
        account = updated
        client = Client(updated.endpoint, tokens.accessToken, BuildConfig.DEBUG)
    }

    private suspend fun restoreLocalState(current: Client, session: SessionView) {
        val goalsKey = key("goals")
        val cache = withContext(Dispatchers.IO) { store.read(goalsKey) }
        if (state.value.goals.isEmpty() && cache != null) {
            val goals = current.restoreCachedGoals(session.agentId, cache)
            stateFlow.update { it.copy(goals = goals) }
        }
        if (delivery == null) {
            val pendingKey = key("pending")
            val encoded = withContext(Dispatchers.IO) { store.read(pendingKey) }
            if (encoded != null) {
                val restored = StoredDelivery.decode(encoded)
                if (restored.command.agentId() == session.agentId) delivery = restored
            }
        }
        stateFlow.update { it.copy(hasPending = delivery != null, pendingText = delivery?.text) }
    }

    private suspend fun persistGoals(current: Client, session: SessionView) {
        val encoded = current.cachedGoals(session.agentId)
        val goalsKey = key("goals")
        withContext(Dispatchers.IO) { store.write(goalsKey, encoded) }
    }

    fun selectGoal(goalId: String?) {
        queueTranscriptCache(state.value, state.value.transcript)
        focus?.cancel()
        goalObservation?.cancel()
        conversationObservation?.cancel()
        goalConnected = false
        conversationConnected = false
        goalObservationFailed = false
        conversationObservationFailed = false
        val changed = state.value.selectedGoalId != goalId
        val placeholder = StoredDraft.empty()
        val draftKey = key("draft:$goalId")
        val loadDraft = draftFocusKey != draftKey
        draftFocusKey = draftKey
        stateFlow.update {
            it.copy(
                selectedGoalId = goalId,
                draft = if (loadDraft) "" else it.draft,
                draftRevision = if (loadDraft) placeholder.revision else it.draftRevision,
                loadingDraft = loadDraft,
                loadingGoal = goalId != null && it.goals.none { goal -> goal.goalId == goalId },
                error = null,
                transcript = if (changed) emptyList() else it.transcript,
                live = if (changed) null else it.live,
                historyBefore = if (changed) null else it.historyBefore,
                historyCursor = if (changed) null else it.historyCursor,
                historyStarted = if (changed) false else it.historyStarted,
                observation = ObservationConnection.CONNECTING,
                observationError = null,
                transcriptSynchronizedAtMs = if (changed) null else it.transcriptSynchronizedAtMs,
            )
        }
        focus = accountScope.launch {
            try {
                if (loadDraft) {
                    draftWriter.flush()
                    val draft =
                        withContext(Dispatchers.IO) {
                            store.read(draftKey)?.let(StoredDraft::decode)
                        } ?: placeholder
                    if (
                        state.value.selectedGoalId == goalId &&
                            state.value.draftRevision == placeholder.revision
                    ) {
                        stateFlow.update {
                            it.copy(
                                draft = draft.text,
                                draftRevision = draft.revision,
                                loadingDraft = false,
                            )
                        }
                    }
                }
                val current = client
                val session = state.value.session
                if (goalId != null && session != null && state.value.transcript.isEmpty()) {
                    try {
                        transcriptWriter.flush()
                        val namespace = transcriptKey(state.value.endpoint, session.ownerId, goalId)
                        val cached = withContext(Dispatchers.IO) { transcriptStore.read(namespace) }
                        val expected =
                            state.value.goals.firstOrNull { it.goalId == goalId }?.conversationId
                        if (
                            cached != null &&
                                expected == cached.metadata.conversationId &&
                                state.value.selectedGoalId == goalId &&
                                state.value.transcriptSynchronizedAtMs == null
                        ) {
                            stateFlow.update {
                                it.copy(
                                    transcript = cached.entries,
                                    live = cached.metadata.live,
                                    transcriptSynchronizedAtMs = cached.metadata.synchronizedAtMs,
                                    historyBefore = cached.metadata.olderBefore,
                                    historyCursor = cached.metadata.cursor,
                                    historyStarted = cached.metadata.historyStarted,
                                    observation = ObservationConnection.OFFLINE,
                                )
                            }
                        }
                    } catch (failure: Exception) {
                        report(failure, R.string.error_storage)
                    }
                }
                if (
                    goalId != null &&
                        current != null &&
                        session != null &&
                        state.value.connection == Connection.ONLINE &&
                        foreground
                ) {
                    val goal = current.goal(session.agentId, goalId)
                    stateFlow.update {
                        it.copy(
                            goals =
                                it.goals.filterNot { previous -> previous.goalId == goalId } + goal
                        )
                    }
                    persistGoals(current, session)
                    startObserving(goal, session)
                }
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
                if (
                    state.value.selectedGoalId == goalId &&
                        state.value.draftRevision == placeholder.revision
                )
                    stateFlow.update { it.copy(loadingDraft = false) }
            } finally {
                if (state.value.selectedGoalId == goalId)
                    stateFlow.update { it.copy(loadingGoal = false) }
            }
        }
    }

    private fun startObserving(goal: GoalView, session: SessionView) {
        if (!foreground) return
        goalObservation?.cancel()
        conversationObservation?.cancel()
        goalObservation = accountScope.launch {
            var retryDelay = 1_000L
            while (isActive && state.value.selectedGoalId == goal.goalId) {
                var observation: dev.gesang.nativeclient.GoalObservation? = null
                try {
                    observation = client?.observeGoal(session.agentId, goal.goalId) ?: return@launch
                    while (isActive) {
                        val next = observation.next()
                        retryDelay = 1_000L
                        goalConnected = true
                        goalObservationFailed = false
                        updateObservationConnection()
                        stateFlow.update {
                            it.copy(
                                goals =
                                    it.goals.filterNot { previous ->
                                        previous.goalId == next.goalId
                                    } + next
                            )
                        }
                    }
                } catch (failure: Exception) {
                    if (failure is CancellationException) throw failure
                    goalConnected = false
                    goalObservationFailed = true
                    updateObservationConnection()
                    if (!recoverObservation(failure)) return@launch
                } finally {
                    withContext(NonCancellable) {
                        observation?.disconnect()
                        observation?.close()
                    }
                }
                delay(retryDelay)
                retryDelay = (retryDelay * 2).coerceAtMost(30_000L)
            }
        }
        conversationObservation = accountScope.launch {
            var retryDelay = 1_000L
            while (isActive && state.value.selectedGoalId == goal.goalId) {
                var observation: dev.gesang.nativeclient.ConversationObservation? = null
                try {
                    observation =
                        client?.observeConversation(session.agentId, goal.conversationId)
                            ?: return@launch
                    while (isActive) {
                        val next = observation.next()
                        retryDelay = 1_000L
                        conversationConnected = true
                        conversationObservationFailed = false
                        stateFlow.update { old ->
                            val visible = next.visibleEntryIds.toSet()
                            val disconnectedWindow =
                                old.transcript.isNotEmpty() &&
                                    visible.isNotEmpty() &&
                                    old.transcript.none { it.entryId in visible }
                            val previous = if (disconnectedWindow) emptyList() else old.transcript
                            val received = next.entries.mapTo(mutableSetOf()) { it.entryId }
                            old.copy(
                                transcript =
                                    (previous.filterNot { it.entryId in received } + next.entries)
                                        .sortedBy { it.entryId },
                                live = next.live,
                                transcriptSynchronizedAtMs = next.synchronizedAtMs,
                                historyBefore =
                                    if (disconnectedWindow || !old.historyStarted) next.olderBefore
                                    else old.historyBefore,
                                historyStarted =
                                    if (disconnectedWindow) false else old.historyStarted,
                                historyCursor = if (disconnectedWindow) null else old.historyCursor,
                            )
                        }
                        if (
                            next.revision == 0uL ||
                                next.entries.isNotEmpty() ||
                                state.value.goal?.phase in
                                    setOf(GoalPhase.COMPLETED, GoalPhase.STOPPED, GoalPhase.FAILED)
                        )
                            queueTranscriptCache(state.value, next.entries)
                        updateObservationConnection()
                    }
                } catch (failure: Exception) {
                    if (failure is CancellationException) throw failure
                    conversationConnected = false
                    conversationObservationFailed = true
                    updateObservationConnection()
                    if (!recoverObservation(failure)) return@launch
                } finally {
                    withContext(NonCancellable) {
                        observation?.disconnect()
                        observation?.close()
                    }
                }
                delay(retryDelay)
                retryDelay = (retryDelay * 2).coerceAtMost(30_000L)
            }
        }
    }

    fun foreground() {
        foreground = true
        if (state.value.session != null) refresh()
    }

    fun background() {
        foreground = false
        goalObservation?.cancel()
        conversationObservation?.cancel()
        stateFlow.update { it.copy(observation = ObservationConnection.OFFLINE) }
        accountScope.launch {
            try {
                draftWriter.flush()
                queueTranscriptCache(state.value, state.value.transcript)
                transcriptWriter.flush()
                val current = client
                val session = state.value.session
                if (current != null && session != null) persistGoals(current, session)
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
            }
        }
    }

    private fun updateObservationConnection() {
        val connected = goalConnected && conversationConnected
        stateFlow.update {
            it.copy(
                observation =
                    if (connected) ObservationConnection.LIVE
                    else if (goalObservationFailed || conversationObservationFailed)
                        ObservationConnection.OFFLINE
                    else ObservationConnection.CONNECTING,
                observationError = if (connected) null else it.observationError,
            )
        }
    }

    private suspend fun recoverObservation(failure: Exception): Boolean {
        if (failure is ClientException.AuthenticationRequired) {
            refresh()
            synchronization?.join()
            return state.value.connection != Connection.SIGNED_OUT
        }
        val error =
            when (failure) {
                is ClientException.PermissionDenied -> R.string.error_permission
                is ClientException.NotFound -> R.string.error_response
                is ClientException.InvalidResponse -> R.string.error_response
                is ClientException.Rejected -> R.string.error_response
                else -> null
            }
        if (error != null) stateFlow.update { it.copy(observationError = error) }
        return error == null
    }

    fun loadOlderMessages(rows: ULong) {
        val snapshot = state.value
        val goal = snapshot.goal ?: return
        val session = snapshot.session ?: return
        val current = client ?: return
        if (
            rows == 0uL ||
                snapshot.loadingHistory ||
                (snapshot.historyStarted && snapshot.historyCursor == null) ||
                (!snapshot.historyStarted && snapshot.historyBefore == null)
        )
            return
        accountScope.launch {
            stateFlow.update { it.copy(loadingHistory = true) }
            try {
                var cursor = snapshot.historyCursor
                val seen = mutableSetOf<String>()
                cursor?.let(seen::add)
                var page: dev.gesang.nativeclient.TranscriptPageView
                do {
                    page =
                        current.transcriptPage(
                            session.agentId,
                            goal.conversationId,
                            rows,
                            cursor,
                            if (snapshot.historyStarted) null else snapshot.historyBefore,
                        )
                    cursor = page.cursor
                    if (cursor != null && !seen.add(cursor))
                        error("The source history cursor did not advance.")
                } while (page.entries.isEmpty() && cursor != null)
                if (state.value.selectedGoalId == goal.goalId)
                    stateFlow.update { old ->
                        val existing = old.transcript.mapTo(mutableSetOf()) { it.entryId }
                        old.copy(
                            transcript =
                                (old.transcript + page.entries.filterNot { it.entryId in existing })
                                    .sortedBy { it.entryId },
                            historyCursor = cursor,
                            historyStarted = true,
                        )
                    }
                if (state.value.selectedGoalId == goal.goalId)
                    queueTranscriptCache(state.value, page.entries)
            } catch (failure: Exception) {
                report(failure)
            } finally {
                if (state.value.selectedGoalId == goal.goalId)
                    stateFlow.update { it.copy(loadingHistory = false) }
            }
        }
    }

    private fun queueTranscriptCache(snapshot: AppState, changed: List<TranscriptEntryView>) {
        val goal = snapshot.goal ?: return
        val session = snapshot.session ?: return
        val synchronized = snapshot.transcriptSynchronizedAtMs ?: return
        val metadata =
            TranscriptCacheMetadata(
                goal.conversationId,
                snapshot.transcript.map { it.entryId },
                snapshot.historyBefore,
                snapshot.historyCursor,
                snapshot.historyStarted,
                synchronized,
                snapshot.live,
            )
        transcriptWriter.write(
            transcriptKey(snapshot.endpoint, session.ownerId, goal.goalId),
            metadata,
            changed,
        )
    }

    private fun transcriptKey(endpoint: String, owner: String, goalId: String): String =
        accountRecordKey(endpoint, owner, "transcript:$goalId")

    fun loadLearning() {
        val current = client ?: return
        val session = state.value.session ?: return
        if (state.value.loadingLearning) return
        accountScope.launch {
            stateFlow.update { it.copy(loadingLearning = true, learningError = null) }
            try {
                val learning = current.learning(session.agentId)
                stateFlow.update { it.copy(learning = learning) }
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update { it.copy(learningError = R.string.error_unavailable) }
            } finally {
                stateFlow.update { it.copy(loadingLearning = false) }
            }
        }
    }

    fun learningAction(action: LearningAction) {
        val current = client ?: return
        val session = state.value.session ?: return
        if (state.value.learningBusy) return
        accountScope.launch {
            stateFlow.update { it.copy(learningBusy = true, learningError = null) }
            try {
                current.actOnLearning(session.agentId, action)
                loadLearning()
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update { it.copy(learningError = R.string.learning_outcome_unknown) }
            } finally {
                stateFlow.update { it.copy(learningBusy = false) }
            }
        }
    }

    fun saveLearningSettings(change: LearningSettingsChange) {
        val current = client ?: return
        val session = state.value.session ?: return
        if (state.value.learningBusy) return
        accountScope.launch {
            stateFlow.update { it.copy(learningBusy = true, learningError = null) }
            try {
                current.updateLearningSettings(session.agentId, change)
                loadLearning()
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update { it.copy(learningError = R.string.learning_outcome_unknown) }
            } finally {
                stateFlow.update { it.copy(learningBusy = false) }
            }
        }
    }

    fun refreshNotifications() {
        if (!BuildConfig.MESSAGING_CONFIGURED) return
        val saved = account?.takeIf { it.ownerId != null } ?: return
        accountScope.launch {
            try {
                val preference = withContext(Dispatchers.IO) { messaging.preference(saved) }
                when {
                    preference.revocationPending && !preference.enabled ->
                        stateFlow.update { it.copy(pushPhase = PushPhase.REVOCATION_PENDING) }
                    !preference.enabled -> stateFlow.update { it.copy(pushPhase = PushPhase.OFF) }
                    !messaging.systemPermissionGranted() ->
                        stateFlow.update { it.copy(pushPhase = PushPhase.PERMISSION_REQUIRED) }
                    else -> {
                        stateFlow.update {
                            it.copy(pushPhase = PushPhase.REGISTERING, pushError = null)
                        }
                        messaging.register()
                        synchronizePush()
                    }
                }
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update {
                    it.copy(
                        pushPhase = PushPhase.SERVER_PENDING,
                        pushError = R.string.notification_registration_failed,
                    )
                }
            }
        }
    }

    fun enableNotifications() {
        val saved = account?.takeIf { it.ownerId != null } ?: return
        if (!BuildConfig.MESSAGING_CONFIGURED || signingOut) return
        accountScope.launch {
            try {
                if (!messaging.systemPermissionGranted()) {
                    stateFlow.update { it.copy(pushPhase = PushPhase.PERMISSION_REQUIRED) }
                    return@launch
                }
                withContext(Dispatchers.IO) {
                    val preference = messaging.preference(saved)
                    messaging.save(
                        saved,
                        preference.copy(revision = UUID.randomUUID().toString(), enabled = true),
                    )
                }
                stateFlow.update { it.copy(pushPhase = PushPhase.REGISTERING, pushError = null) }
                messaging.register()
                synchronizePush()
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update {
                    it.copy(
                        pushPhase = PushPhase.SERVER_PENDING,
                        pushError = R.string.notification_registration_failed,
                    )
                }
            }
        }
    }

    private fun synchronizePush() {
        if (!BuildConfig.MESSAGING_CONFIGURED || signingOut || account?.ownerId == null) return
        pushDirty = true
        if (pushSynchronization?.isActive == true) return
        pushSynchronization = accountScope.launch {
            do {
                pushDirty = false
                val saved = account ?: return@launch
                val current = client ?: return@launch
                try {
                    val preference = withContext(Dispatchers.IO) { messaging.preference(saved) }
                    if (!preference.enabled || !messaging.systemPermissionGranted()) return@launch
                    val fid = withContext(Dispatchers.IO) { messaging.installationId() }
                    val project = messaging.projectId()
                    if (
                        fid == null ||
                            project == null ||
                            state.value.connection != Connection.ONLINE
                    ) {
                        stateFlow.update { it.copy(pushPhase = PushPhase.SERVER_PENDING) }
                        return@launch
                    }
                    stateFlow.update {
                        it.copy(pushPhase = PushPhase.SERVER_PENDING, pushError = null)
                    }
                    current.registerDevicePush(preference.deviceId, project, fid)
                    val applied =
                        withContext(Dispatchers.IO) {
                            val latest = messaging.preference(saved)
                            if (
                                latest.revision == preference.revision &&
                                    latest.enabled &&
                                    messaging.installationId() == fid
                            ) {
                                messaging.save(
                                    saved,
                                    latest.copy(
                                        registeredInstallationId = fid,
                                        revocationPending = false,
                                    ),
                                )
                                true
                            } else false
                        }
                    if (applied) stateFlow.update { it.copy(pushPhase = PushPhase.REGISTERED) }
                } catch (failure: Exception) {
                    if (failure is CancellationException) throw failure
                    stateFlow.update {
                        it.copy(
                            pushPhase = PushPhase.SERVER_PENDING,
                            pushError = R.string.notification_registration_failed,
                        )
                    }
                }
            } while (pushDirty)
        }
    }

    fun disableNotifications() {
        val saved = account ?: return
        val current = client ?: return
        if (signingOut) return
        accountScope.launch {
            try {
                val preference = withContext(Dispatchers.IO) { messaging.disableLocal(saved) }
                stateFlow.update {
                    it.copy(pushPhase = PushPhase.REVOCATION_PENDING, pushError = null)
                }
                messaging.stopProvider()
                val revoked =
                    withTimeoutOrNull(5_000L) {
                        current.revokeDevicePush(preference.deviceId)
                        true
                    } == true
                if (revoked) {
                    withContext(Dispatchers.IO) {
                        messaging.save(
                            saved,
                            preference.copy(
                                revocationPending = false,
                                registeredInstallationId = null,
                            ),
                        )
                    }
                    stateFlow.update { it.copy(pushPhase = PushPhase.OFF) }
                }
            } catch (failure: Exception) {
                if (failure is CancellationException) throw failure
                stateFlow.update {
                    it.copy(
                        pushPhase = PushPhase.REVOCATION_PENDING,
                        pushError = R.string.notification_revocation_pending,
                    )
                }
            }
        }
    }

    fun openNotification(agentId: String, goalId: String) {
        notificationDestination = NotificationDestination(agentId, goalId)
        refresh()
    }

    private fun openNotificationDestination(session: SessionView) {
        val destination = notificationDestination ?: return
        notificationDestination = null
        if (destination.agentId == session.agentId) selectGoal(destination.goalId)
        else stateFlow.update { it.copy(error = R.string.error_notification_account) }
    }

    fun editDraft(text: String) {
        if (state.value.loadingDraft) return
        val draft = StoredDraft(UUID.randomUUID().toString(), text)
        stateFlow.update { it.copy(draft = text, draftRevision = draft.revision) }
        if (account?.ownerId != null)
            draftWriter.write(key("draft:${state.value.selectedGoalId}"), draft)
    }

    fun selectPermission(permission: Permission) {
        val current = client ?: return
        val session = state.value.session ?: return
        if (state.value.busy) return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                val grant = current.selectPermission(session.agentId, permission, emptyList())
                stateFlow.update { it.copy(session = session.copy(grant = grant)) }
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun send() {
        val snapshot = state.value
        val current = client ?: return
        val session = snapshot.session ?: return
        if (
            snapshot.connection != Connection.ONLINE ||
                snapshot.busy ||
                delivery != null ||
                snapshot.loadingDraft ||
                snapshot.draft.isBlank()
        )
            return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                val goal = snapshot.goal
                if (goal?.phase == GoalPhase.FAILED) {
                    stateFlow.update { it.copy(error = R.string.terminal_new_conversation) }
                    return@launch
                }
                val command =
                    if (goal == null) {
                        val grant = session.grant ?: return@launch
                        current.prepareGoal(session.agentId, grant, snapshot.draft, emptyList())
                    } else if (
                        goal.phase == GoalPhase.COMPLETED || goal.phase == GoalPhase.STOPPED
                    ) {
                        val grant = session.grant ?: return@launch
                        current.prepareContinuation(
                            session.agentId,
                            grant,
                            goal.goalId,
                            goal.version,
                            snapshot.draft,
                            emptyList(),
                        )
                    } else
                        current.prepareInput(
                            session.agentId,
                            goal.goalId,
                            goal.version,
                            snapshot.draft,
                            goal.questionId,
                        )
                persistBeforeDelivery(command, snapshot)
                val receipt = current.submit(command)
                applyReceipt(receipt)
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    private suspend fun persistBeforeDelivery(
        command: PendingCommand,
        draft: AppState? = null,
        sourceGoalId: String? = draft?.selectedGoalId,
    ) {
        val record =
            StoredDelivery(
                command,
                draft?.let { key("draft:${it.selectedGoalId}") },
                draft?.draftRevision,
                sourceGoalId,
                draft?.draft,
            )
        val pendingKey = key("pending")
        withContext(Dispatchers.IO) { store.write(pendingKey, record.encode()) }
        delivery = record
        stateFlow.update {
            it.copy(hasPending = true, pendingText = record.text, pendingRejected = false)
        }
    }

    private suspend fun reconcilePending() {
        val command = delivery?.command ?: return
        val current = client ?: return
        val receipt =
            try {
                current.requestStatus(command.requestId())
            } catch (_: ClientException.NotFound) {
                current.submit(command)
            }
        applyReceipt(receipt)
    }

    private suspend fun applyReceipt(receipt: dev.gesang.nativeclient.ReceiptView) {
        val pending = delivery?.command ?: throw ClientException.InvalidResponse()
        if (receipt.requestId != pending.requestId() || receipt.agentId != pending.agentId()) {
            throw ClientException.InvalidResponse()
        }
        if (receipt.state == ReceiptState.PENDING) return
        val current = client ?: return
        val session = state.value.session ?: return
        if (receipt.state == ReceiptState.REJECTED) {
            stateFlow.update { it.copy(error = R.string.receipt_rejected, pendingRejected = true) }
            return
        } else {
            val goalId = receipt.goalId
            if (goalId != null) {
                val goal = current.goal(session.agentId, goalId)
                val original = delivery
                val beforeReceipt = state.value
                val selectedOrigin = beforeReceipt.selectedGoalId == original?.draftGoalId
                stateFlow.update { old ->
                    val clearDraft =
                        original != null &&
                            original.draftRevision == old.draftRevision &&
                            original.draftGoalId == old.selectedGoalId
                    old.copy(
                        goals = old.goals.filterNot { it.goalId == goalId } + goal,
                        selectedGoalId =
                            if (old.selectedGoalId == original?.draftGoalId) goalId
                            else old.selectedGoalId,
                        draft = if (clearDraft) "" else old.draft,
                        draftRevision =
                            if (clearDraft) UUID.randomUUID().toString() else old.draftRevision,
                    )
                }
                if (original?.draftKey != null && original.draftRevision != null)
                    draftWriter.clearIfUnchanged(original.draftKey, original.draftRevision)
                draftWriter.flush()
                if (
                    original?.draftKey != null &&
                        selectedOrigin &&
                        beforeReceipt.selectedGoalId != goalId &&
                        beforeReceipt.draftRevision != original.draftRevision
                ) {
                    draftWriter.write(
                        key("draft:$goalId"),
                        StoredDraft(beforeReceipt.draftRevision, beforeReceipt.draft),
                    )
                    draftWriter.flush()
                    draftWriter.clearIfUnchanged(original.draftKey, beforeReceipt.draftRevision)
                    draftWriter.flush()
                }
                persistGoals(current, session)
                if (state.value.selectedGoalId == goalId) startObserving(goal, session)
            }
        }
        val pendingKey = key("pending")
        withContext(Dispatchers.IO) { store.clear(pendingKey) }
        delivery = null
        stateFlow.update {
            it.copy(hasPending = false, pendingText = null, pendingRejected = false)
        }
    }

    fun stopGoal() {
        val snapshot = state.value
        val current = client ?: return
        val session = snapshot.session ?: return
        val goal = snapshot.goal ?: return
        if (snapshot.busy || delivery != null) return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                val command = current.prepareStop(session.agentId, goal.goalId, goal.version)
                persistBeforeDelivery(command, sourceGoalId = goal.goalId)
                applyReceipt(current.submit(command))
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun resumeGoal(text: String) {
        val current = client ?: return
        val snapshot = state.value
        val session = snapshot.session ?: return
        val grant = session.grant ?: return
        val goal = snapshot.goal ?: return
        if (
            snapshot.busy ||
                delivery != null ||
                snapshot.connection != Connection.ONLINE ||
                goal.phase != GoalPhase.WAITING_AUTHORIZATION
        )
            return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                val latest = current.goal(session.agentId, goal.goalId)
                if (latest.phase != GoalPhase.WAITING_AUTHORIZATION)
                    throw ClientException.Conflict()
                val command =
                    current.prepareResume(
                        session.agentId,
                        grant,
                        latest.goalId,
                        latest.version,
                        text,
                        latest.questionId,
                    )
                persistBeforeDelivery(command, sourceGoalId = goal.goalId)
                applyReceipt(current.submit(command))
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun revoke() {
        if (state.value.busy || signingOut) return
        val session = state.value.session ?: return
        val grant = session.grant ?: return
        val current = client ?: return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true, error = null) }
            try {
                current.revokeGrant(session.agentId, grant.id)
                stateFlow.update { it.copy(session = session.copy(grant = null)) }
            } catch (failure: Exception) {
                report(failure)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun signOut() {
        if (signingOut) return
        signingOut = true
        stateFlow.update { it.copy(busy = true) }
        queueTranscriptCache(state.value, state.value.transcript)
        val departing = account
        val departingClient = client
        client = null
        synchronization?.cancel()
        viewModelScope.launch {
            try {
                val push =
                    if (departing?.ownerId != null)
                        withContext(Dispatchers.IO) { messaging.disableLocal(departing) }
                    else null
                bootstrap?.cancelAndJoin()
                authorizationJob?.cancelAndJoin()
                accountJob.cancelAndJoin()
                withContext(Dispatchers.IO) {
                    store.clear("account")
                    store.clear("authorization")
                }
                account = null
                attempt = null
                attemptEndpoint = null
                var revocationPending = push?.revocationPending == true
                if (
                    push?.revocationPending == true && departingClient != null && departing != null
                ) {
                    try {
                        if (
                            withTimeoutOrNull(5_000L) {
                                departingClient.revokeDevicePush(push.deviceId)
                                true
                            } == true
                        ) {
                            withContext(Dispatchers.IO) {
                                messaging.save(
                                    departing,
                                    push.copy(
                                        revocationPending = false,
                                        registeredInstallationId = null,
                                    ),
                                )
                            }
                            revocationPending = false
                        }
                    } catch (failure: Exception) {
                        if (failure is CancellationException) throw failure
                        // Keep the server revocation visibly pending; never retain credentials for
                        // it.
                    }
                }
                messaging.stopProvider()
                draftWriter.flush()
                transcriptWriter.flush()
                client = null
                delivery = null
                stateFlow.value =
                    AppState(
                        endpoint = state.value.endpoint,
                        error =
                            if (revocationPending) R.string.notification_revocation_pending
                            else null,
                    )
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
            } finally {
                signingOut = false
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun editRejectedDelivery() {
        val original = delivery ?: return
        if (!state.value.pendingRejected || original.text == null || state.value.loadingDraft)
            return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true) }
            try {
                editDraft(original.text)
                draftWriter.flush()
                val pendingKey = key("pending")
                withContext(Dispatchers.IO) { store.clear(pendingKey) }
                delivery = null
                stateFlow.update {
                    it.copy(
                        hasPending = false,
                        pendingRejected = false,
                        pendingText = null,
                        error = null,
                    )
                }
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    fun discardRejectedDelivery() {
        if (!state.value.pendingRejected || state.value.busy) return
        accountScope.launch {
            stateFlow.update { it.copy(busy = true) }
            try {
                val pendingKey = key("pending")
                withContext(Dispatchers.IO) { store.clear(pendingKey) }
                delivery = null
                stateFlow.update {
                    it.copy(
                        hasPending = false,
                        pendingRejected = false,
                        pendingText = null,
                        error = null,
                    )
                }
            } catch (failure: Exception) {
                report(failure, R.string.error_storage)
            } finally {
                stateFlow.update { it.copy(busy = false) }
            }
        }
    }

    private fun key(record: String): String {
        return accountRecordKey(account?.endpoint, account?.ownerId, record)
    }

    private fun report(failure: Exception, fallback: Int = R.string.error_unavailable) {
        if (failure is CancellationException) throw failure
        val error =
            when (failure) {
                is ClientException.AuthenticationRequired -> R.string.error_authentication
                is ClientException.PermissionDenied -> R.string.error_permission
                is ClientException.Conflict -> R.string.error_conflict
                is ClientException.InvalidAddress -> R.string.error_address
                is ClientException.InvalidResponse -> R.string.error_response
                is ClientException.ConnectionUnavailable -> R.string.error_connection
                else -> fallback
            }
        stateFlow.update { previous ->
            previous.copy(
                error = error,
                connection =
                    when (failure) {
                        is ClientException.AuthenticationRequired -> Connection.SIGNED_OUT
                        is ClientException.ConnectionUnavailable -> Connection.OFFLINE
                        else -> previous.connection
                    },
            )
        }
    }
}
