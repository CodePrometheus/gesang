package dev.gesang.app.ui

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.selection.toggleable
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import dev.gesang.app.AppState
import dev.gesang.app.AppViewModel
import dev.gesang.app.R
import dev.gesang.app.ui.theme.LocalSurfaceColors
import dev.gesang.nativeclient.LearningAction
import dev.gesang.nativeclient.LearningSettingsChange
import dev.gesang.nativeclient.LearningSettingsView
import dev.gesang.nativeclient.NumberChange
import dev.gesang.nativeclient.TextChange
import java.math.BigDecimal

/**
 * Owner actions stay explicit; this screen never fabricates an evaluation or successful adoption.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun LearningScreen(state: AppState, model: AppViewModel, onBack: () -> Unit) {
    var decision by remember { mutableStateOf<LearningAction?>(null) }
    var settings by remember { mutableStateOf(false) }
    val learning = state.learning
    Scaffold(
        containerColor = MaterialTheme.colorScheme.surface,
        topBar = {
            TopAppBar(
                title = {
                    Text(
                        stringResource(R.string.improvement),
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
                actions = {
                    IconButton(onClick = model::loadLearning, enabled = !state.loadingLearning) {
                        AppIcon(
                            R.drawable.ic_refresh_cw,
                            description = stringResource(R.string.refresh),
                        )
                    }
                },
            )
        },
    ) { padding ->
        LazyColumn(
            Modifier.fillMaxSize().padding(padding),
            contentPadding = PaddingValues(horizontal = 24.dp, vertical = 20.dp),
            verticalArrangement = Arrangement.spacedBy(20.dp),
        ) {
            item {
                Text(
                    stringResource(R.string.improvement_body),
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            state.learningError?.let { error ->
                item {
                    Text(
                        stringResource(error),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
            }
            if (state.loadingLearning)
                item { CircularProgressIndicator(Modifier.size(24.dp), strokeWidth = 2.dp) }
            if (learning != null) {
                item {
                    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                        Text(
                            stringResource(R.string.learning_spent, decimal(learning.spentUsd)),
                            style = MaterialTheme.typography.titleSmall,
                        )
                        Text(
                            stringResource(
                                R.string.learning_counts,
                                learning.inUse.toString(),
                                learning.archived.toString(),
                            ),
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                        TextButton(
                            onClick = { settings = true },
                            contentPadding = PaddingValues(0.dp),
                        ) {
                            Text(stringResource(R.string.learning_settings))
                        }
                    }
                }
                if (learning.held.isNotEmpty()) {
                    item { LearningHeading(stringResource(R.string.learning_review)) }
                    items(learning.held, key = { "held:${it.id}" }) { held ->
                        Surface(
                            color = LocalSurfaceColors.current.card,
                            shape = RoundedCornerShape(20.dp),
                        ) {
                            Column(
                                Modifier.padding(18.dp),
                                verticalArrangement = Arrangement.spacedBy(12.dp),
                            ) {
                                Text(held.text, style = MaterialTheme.typography.bodyLarge)
                                DetailDisclosure(
                                    stringResource(R.string.learning_evidence),
                                    (listOf(held.failure, held.prediction) + held.reasons)
                                        .filter(String::isNotBlank)
                                        .joinToString("\n\n"),
                                )
                                Text(
                                    stringResource(R.string.learning_verify_body),
                                    style = MaterialTheme.typography.bodySmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                    TextButton(
                                        onClick = { decision = LearningAction.Approve(held.id) },
                                        enabled = !state.learningBusy,
                                    ) {
                                        Text(stringResource(R.string.learning_allow))
                                    }
                                    TextButton(
                                        onClick = { decision = LearningAction.Reject(held.id) },
                                        enabled = !state.learningBusy,
                                    ) {
                                        Text(stringResource(R.string.learning_reject))
                                    }
                                }
                            }
                        }
                    }
                }
                if (learning.corrections.isNotEmpty()) {
                    item { LearningHeading(stringResource(R.string.learning_corrections)) }
                    items(learning.corrections, key = { "correction:${it.key}" }) { correction ->
                        Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                            Text(correction.key, style = MaterialTheme.typography.titleSmall)
                            Text(
                                stringResource(
                                    R.string.learning_sessions,
                                    correction.sessions.toString(),
                                ),
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            correction.examples.forEach { MessageMarkdown(it) }
                            FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                TextButton(
                                    onClick = {
                                        decision =
                                            LearningAction.AnswerCorrection(correction.key, true)
                                    },
                                    enabled = !state.learningBusy,
                                ) {
                                    Text(stringResource(R.string.learning_learn_kind))
                                }
                                TextButton(
                                    onClick = {
                                        decision =
                                            LearningAction.AnswerCorrection(correction.key, false)
                                    },
                                    enabled = !state.learningBusy,
                                ) {
                                    Text(stringResource(R.string.learning_keep_kind))
                                }
                            }
                            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
                        }
                    }
                }
                if (learning.changes.isNotEmpty()) {
                    item { LearningHeading(stringResource(R.string.learning_changes)) }
                    items(learning.changes, key = { "change:${it.id}" }) { change ->
                        Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                            Text(
                                stringResource(
                                    if (change.archived) R.string.learning_archived
                                    else R.string.learning_active
                                ),
                                style = MaterialTheme.typography.labelMedium,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            MessageMarkdown(change.text)
                            Text(
                                change.added,
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            if (!change.archived)
                                TextButton(
                                    onClick = { decision = LearningAction.Undo(change.id) },
                                    enabled = !state.learningBusy,
                                ) {
                                    Text(stringResource(R.string.learning_undo))
                                }
                            HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
                        }
                    }
                }
                if (
                    learning.held.isEmpty() &&
                        learning.corrections.isEmpty() &&
                        learning.changes.isEmpty()
                )
                    item {
                        Text(
                            stringResource(R.string.learning_none),
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
            }
        }
    }
    decision?.let { action ->
        AlertDialog(
            onDismissRequest = { decision = null },
            shape = RoundedCornerShape(24.dp),
            title = {
                Text(
                    stringResource(
                        if (action is LearningAction.Approve) R.string.learning_allow
                        else R.string.confirm_action
                    )
                )
            },
            text = {
                Text(
                    stringResource(
                        if (action is LearningAction.Approve) R.string.learning_approve_body
                        else if (action is LearningAction.Undo) R.string.learning_undo_body
                        else R.string.learning_decision_body
                    )
                )
            },
            confirmButton = {
                TextButton(
                    onClick = {
                        decision = null
                        model.learningAction(action)
                    }
                ) {
                    Text(stringResource(R.string.confirm_action))
                }
            },
            dismissButton = {
                TextButton(onClick = { decision = null }) { Text(stringResource(R.string.cancel)) }
            },
        )
    }
    if (settings && learning != null)
        LearningSettings(learning.settings, state.learningBusy, { settings = false }) { change ->
            settings = false
            model.saveLearningSettings(change)
        }
}

@Composable
private fun LearningHeading(title: String) {
    Text(
        title,
        style = MaterialTheme.typography.titleMedium,
        modifier = Modifier.padding(top = 12.dp).semantics { heading() },
    )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun LearningSettings(
    current: LearningSettingsView,
    busy: Boolean,
    onDismiss: () -> Unit,
    onSave: (LearningSettingsChange) -> Unit,
) {
    var budget by
        rememberSaveable(current.weeklyBudgetUsd) {
            mutableStateOf(current.weeklyBudgetUsd?.let(::decimal).orEmpty())
        }
    var regular by
        rememberSaveable(current.regularModel) { mutableStateOf(current.regularModel.orEmpty()) }
    var strong by
        rememberSaveable(current.strongModel) { mutableStateOf(current.strongModel.orEmpty()) }
    var review by rememberSaveable(current.holdForReview) { mutableStateOf(current.holdForReview) }
    val value = budget.trim().takeIf(String::isNotEmpty)?.toDoubleOrNull()
    val valid = budget.isBlank() || (value != null && value.isFinite() && value >= 0.0)
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        containerColor = MaterialTheme.colorScheme.surface,
    ) {
        LazyColumn(
            Modifier.fillMaxWidth().imePadding(),
            contentPadding = PaddingValues(start = 24.dp, end = 24.dp, bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(18.dp),
        ) {
            item { LearningHeading(stringResource(R.string.learning_settings)) }
            item {
                OutlinedTextField(
                    budget,
                    { budget = it },
                    label = { Text(stringResource(R.string.learning_budget)) },
                    placeholder = { Text(stringResource(R.string.learning_no_limit)) },
                    keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Decimal),
                    singleLine = true,
                    isError = !valid,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(12.dp),
                )
                Text(
                    stringResource(R.string.learning_budget_body),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 8.dp),
                )
            }
            item {
                OutlinedTextField(
                    regular,
                    { regular = it },
                    label = { Text(stringResource(R.string.learning_regular_model)) },
                    placeholder = { Text(stringResource(R.string.learning_model_default)) },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(12.dp),
                )
            }
            item {
                OutlinedTextField(
                    strong,
                    { strong = it },
                    label = { Text(stringResource(R.string.learning_strong_model)) },
                    placeholder = { Text(stringResource(R.string.learning_model_default)) },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth(),
                    shape = RoundedCornerShape(12.dp),
                )
            }
            item {
                Row(
                    Modifier.fillMaxWidth()
                        .heightIn(min = 48.dp)
                        .toggleable(
                            value = review,
                            role = Role.Switch,
                            onValueChange = { review = it },
                        ),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(16.dp),
                ) {
                    Text(
                        stringResource(R.string.learning_review),
                        style = MaterialTheme.typography.bodyLarge,
                        modifier = Modifier.weight(1f),
                    )
                    Switch(checked = review, onCheckedChange = null)
                }
            }
            item {
                Button(
                    onClick = {
                        val budgetEdit =
                            if (budget == current.weeklyBudgetUsd?.let(::decimal).orEmpty())
                                NumberChange.Unchanged
                            else if (budget.isBlank()) NumberChange.Clear
                            else NumberChange.Set(value ?: return@Button)
                        fun modelEdit(text: String, old: String?): TextChange =
                            if (text == old.orEmpty()) TextChange.Unchanged
                            else if (text.isBlank()) TextChange.Clear
                            else TextChange.Set(text.trim())
                        onSave(
                            LearningSettingsChange(
                                budgetEdit,
                                modelEdit(regular, current.regularModel),
                                modelEdit(strong, current.strongModel),
                                review.takeIf { it != current.holdForReview },
                            )
                        )
                    },
                    enabled = valid && !busy,
                    modifier = Modifier.fillMaxWidth().heightIn(min = 48.dp),
                    shape = RoundedCornerShape(24.dp),
                ) {
                    Text(stringResource(R.string.save))
                }
            }
        }
    }
}

private fun decimal(value: Double): String =
    BigDecimal.valueOf(value).stripTrailingZeros().toPlainString()
