package dev.gesang.app.ui

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.util.Base64
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalWindowInfo
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import com.mikepenz.markdown.m3.Markdown
import com.mikepenz.markdown.m3.markdownTypography
import com.mikepenz.markdown.model.rememberMarkdownState
import com.mikepenz.markdown.model.rememberStreamingMarkdownState
import dev.gesang.app.R
import dev.gesang.app.ui.theme.LocalSurfaceColors
import dev.gesang.nativeclient.ContentBlock
import dev.gesang.nativeclient.ContentKind
import dev.gesang.nativeclient.LiveToolPhase
import dev.gesang.nativeclient.LiveView
import dev.gesang.nativeclient.MessageRole
import dev.gesang.nativeclient.MessageView
import dev.gesang.nativeclient.TranscriptEntryView
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Render only actual source entries; a live tool phase never becomes a successful result here. */
@Composable
internal fun TranscriptEntry(entry: TranscriptEntryView) {
    Column(verticalArrangement = Arrangement.spacedBy(18.dp)) {
        if (entry.messages.isEmpty()) DetailDisclosure(entry.kind, entry.details.orEmpty())
        entry.messages.forEach { message ->
            when (message.role) {
                MessageRole.USER ->
                    Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                        Surface(
                            color = LocalSurfaceColors.current.bubble,
                            shape =
                                RoundedCornerShape(
                                    topStart = 22.dp,
                                    topEnd = 22.dp,
                                    bottomEnd = 10.dp,
                                    bottomStart = 22.dp,
                                ),
                            modifier = Modifier.padding(start = 16.dp).widthIn(max = 560.dp),
                        ) {
                            Column(Modifier.padding(horizontal = 18.dp, vertical = 15.dp)) {
                                MessageContent(message)
                            }
                        }
                    }
                MessageRole.ASSISTANT -> AgentMessage(message)
                MessageRole.TOOL -> {
                    val label = message.toolName ?: stringResource(R.string.tool_result)
                    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        DetailDisclosure(
                            label,
                            message.content.joinToString("\n") { it.text },
                            failed = message.failed,
                        )
                        message.details?.let {
                            DetailDisclosure(stringResource(R.string.details), it)
                        }
                        message.content
                            .filter { it.kind == ContentKind.IMAGE }
                            .forEach { TranscriptImage(it) }
                    }
                }
                MessageRole.ACTIVITY ->
                    DetailDisclosure(entry.kind, message.content.joinToString("\n") { it.text })
            }
        }
    }
}

@Composable
private fun AgentMessage(message: MessageView, streaming: Boolean = false) {
    Column(verticalArrangement = Arrangement.spacedBy(14.dp)) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            AppIcon(R.drawable.ic_gesang, modifier = Modifier.size(24.dp))
            Text(stringResource(R.string.app_name), style = MaterialTheme.typography.titleSmall)
        }
        MessageContent(message, streaming)
        message.errorMessage?.let {
            Text(
                it,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.error,
            )
        }
    }
}

@Composable
private fun MessageContent(message: MessageView, streaming: Boolean = false) {
    Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
        message.content.forEach { block ->
            when (block.kind) {
                ContentKind.TEXT ->
                    if (message.role == MessageRole.USER) {
                        SelectionContainer {
                            Text(block.text, style = MaterialTheme.typography.bodyLarge)
                        }
                    } else MessageMarkdown(block.text, streaming)
                ContentKind.REASONING ->
                    if (block.text.isNotBlank())
                        DetailDisclosure(stringResource(R.string.reasoning), block.text)
                ContentKind.TOOL_CALL ->
                    DetailDisclosure(
                        block.toolName ?: stringResource(R.string.tool_call),
                        block.text,
                    )
                ContentKind.IMAGE -> TranscriptImage(block)
                ContentKind.UNSUPPORTED ->
                    DetailDisclosure(stringResource(R.string.additional_content), block.text)
            }
        }
    }
}

@Composable
internal fun MessageMarkdown(content: String, streaming: Boolean = false) {
    val typography =
        markdownTypography(
            h1 = MaterialTheme.typography.headlineMedium,
            h2 = MaterialTheme.typography.headlineSmall,
            h3 = MaterialTheme.typography.titleLarge,
            h4 = MaterialTheme.typography.titleMedium,
            h5 = MaterialTheme.typography.titleSmall,
            h6 = MaterialTheme.typography.titleSmall,
            text = MaterialTheme.typography.bodyLarge,
            paragraph = MaterialTheme.typography.bodyLarge,
        )
    SelectionContainer {
        if (streaming) {
            var reset by remember { mutableIntStateOf(0) }
            key(reset) {
                val markdown = rememberStreamingMarkdownState()
                var previous by remember { mutableStateOf("") }
                LaunchedEffect(content) {
                    if (!content.startsWith(previous)) {
                        reset += 1
                    } else {
                        withContext(Dispatchers.Default) {
                            markdown.append(content.substring(previous.length))
                        }
                        previous = content
                    }
                }
                Markdown(markdown, modifier = Modifier.fillMaxWidth(), typography = typography)
            }
        } else {
            val markdown = rememberMarkdownState(content, retainState = true)
            Markdown(
                markdown,
                modifier = Modifier.fillMaxWidth(),
                typography = typography,
                error = { Text(content, style = MaterialTheme.typography.bodyLarge) },
            )
        }
    }
}

@Composable
internal fun LiveProgress(live: LiveView) {
    Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
        live.generation?.let { AgentMessage(it, streaming = true) }
        live.tools.forEach { tool ->
            val phase =
                stringResource(
                    when (tool.phase) {
                        LiveToolPhase.PENDING -> R.string.tool_pending
                        LiveToolPhase.RUNNING -> R.string.tool_running
                        LiveToolPhase.DONE -> R.string.tool_finished
                        LiveToolPhase.UNKNOWN -> R.string.tool_state_unknown
                    }
                )
            DetailDisclosure("${tool.name} · $phase", tool.output.orEmpty())
            if (tool.droppedBytes != null || tool.droppedLines != null) {
                Text(
                    stringResource(R.string.tool_output_retained),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            tool.details?.let { DetailDisclosure(stringResource(R.string.details), it) }
        }
        live.retryError?.let {
            Text(
                it,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        if (live.compacting)
            Text(
                stringResource(R.string.history_compacting),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        live.failureReason?.let { reason ->
            DetailDisclosure(reason, live.failureDetail.orEmpty(), failed = true)
        }
    }
}

@Composable
internal fun DetailDisclosure(title: String, text: String, failed: Boolean = false) {
    var expanded by rememberSaveable { mutableStateOf(false) }
    Column(Modifier.fillMaxWidth()) {
        Surface(
            onClick = { expanded = !expanded },
            color = Color.Transparent,
            shape = RoundedCornerShape(12.dp),
        ) {
            Row(
                Modifier.fillMaxWidth().heightIn(min = 48.dp).padding(vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                AppIcon(
                    if (failed) R.drawable.ic_x else R.drawable.ic_activity,
                    tint =
                        if (failed) MaterialTheme.colorScheme.error
                        else MaterialTheme.colorScheme.onSurfaceVariant,
                )
                Text(
                    title,
                    style = MaterialTheme.typography.bodySmall,
                    color =
                        if (failed) MaterialTheme.colorScheme.error
                        else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.weight(1f),
                )
                AppIcon(
                    if (expanded) R.drawable.ic_chevron_down else R.drawable.ic_chevron_right,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        }
        if (expanded && text.isNotEmpty())
            SelectionContainer {
                Text(
                    text,
                    style =
                        MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                    modifier = Modifier.padding(start = 34.dp, top = 8.dp, bottom = 12.dp),
                )
            }
    }
}

@Composable
private fun TranscriptImage(block: ContentBlock) {
    BoxWithConstraints(Modifier.fillMaxWidth()) {
        val width = with(LocalDensity.current) { maxWidth.roundToPx() }
        val height = LocalWindowInfo.current.containerSize.height
        val bitmap by
            produceState<Bitmap?>(null, block.imageData, width, height) {
                value =
                    withContext(Dispatchers.Default) {
                        try {
                            val data = block.imageData ?: return@withContext null
                            if (data.length.toLong() * 3 / 4 > Runtime.getRuntime().maxMemory() / 8)
                                return@withContext null
                            val bytes = Base64.decode(data, Base64.DEFAULT)
                            val options =
                                BitmapFactory.Options().apply { inJustDecodeBounds = true }
                            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, options)
                            if (options.outWidth <= 0 || options.outHeight <= 0)
                                return@withContext null
                            options.inJustDecodeBounds = false
                            while (
                                options.outWidth / options.inSampleSize.coerceAtLeast(1) >
                                    width.coerceAtLeast(1) * 2 ||
                                    options.outHeight / options.inSampleSize.coerceAtLeast(1) >
                                        height.coerceAtLeast(1) * 2
                            ) {
                                options.inSampleSize = options.inSampleSize.coerceAtLeast(1) * 2
                            }
                            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, options)
                        } catch (_: IllegalArgumentException) {
                            null
                        }
                    }
            }
        val image = bitmap
        if (image != null)
            Image(
                image.asImageBitmap(),
                contentDescription = stringResource(R.string.image_attachment),
                modifier = Modifier.fillMaxWidth().heightIn(max = 480.dp),
                contentScale = ContentScale.Fit,
            )
        else
            Text(
                stringResource(R.string.image_unavailable),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
    }
}
