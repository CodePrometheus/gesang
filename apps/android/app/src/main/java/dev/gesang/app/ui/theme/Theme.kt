package dev.gesang.app.ui.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.Immutable
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.sp

@Immutable
data class SurfaceColors(
    val soft: Color,
    val bubble: Color,
    val card: Color,
    val success: Color,
    val warning: Color,
)

private val LightScheme =
    lightColorScheme(
        primary = Color(0xFF2257CF),
        onPrimary = Color.White,
        primaryContainer = Color(0xFFEDF2FF),
        onPrimaryContainer = Color(0xFF25499C),
        secondary = Color(0xFF25499C),
        onSecondary = Color.White,
        secondaryContainer = Color(0xFFEDF2FF),
        onSecondaryContainer = Color(0xFF25499C),
        tertiary = Color(0xFF62666E),
        onTertiary = Color.White,
        tertiaryContainer = Color(0xFFF4F5F7),
        onTertiaryContainer = Color(0xFF202124),
        background = Color.White,
        onBackground = Color(0xFF202124),
        surface = Color.White,
        onSurface = Color(0xFF202124),
        surfaceVariant = Color(0xFFF4F5F7),
        onSurfaceVariant = Color(0xFF62666E),
        surfaceTint = Color(0xFF2257CF),
        surfaceBright = Color.White,
        surfaceDim = Color(0xFFF0F2F6),
        surfaceContainerLowest = Color.White,
        surfaceContainerLow = Color(0xFFF7F8FA),
        surfaceContainer = Color(0xFFF7F8FA),
        surfaceContainerHigh = Color(0xFFF4F5F7),
        surfaceContainerHighest = Color(0xFFF0F2F6),
        outline = Color(0xFF8A8F98),
        outlineVariant = Color(0xFFE4E6E9),
        error = Color(0xFFB13535),
        onError = Color.White,
        errorContainer = Color(0xFFF4F5F7),
        onErrorContainer = Color(0xFFB13535),
    )

private val DarkScheme =
    darkColorScheme(
        primary = Color(0xFFB8CAFF),
        onPrimary = Color(0xFF172E65),
        primaryContainer = Color(0xFF27334E),
        onPrimaryContainer = Color(0xFFC4D2FF),
        secondary = Color(0xFFC4D2FF),
        onSecondary = Color(0xFF172E65),
        secondaryContainer = Color(0xFF27334E),
        onSecondaryContainer = Color(0xFFC4D2FF),
        tertiary = Color(0xFFB0B0B0),
        onTertiary = Color(0xFF1B1B1B),
        tertiaryContainer = Color(0xFF252525),
        onTertiaryContainer = Color(0xFFEDEDED),
        background = Color(0xFF1B1B1B),
        onBackground = Color(0xFFEDEDED),
        surface = Color(0xFF1B1B1B),
        onSurface = Color(0xFFEDEDED),
        surfaceVariant = Color(0xFF252525),
        onSurfaceVariant = Color(0xFFB0B0B0),
        surfaceTint = Color(0xFFB8CAFF),
        surfaceBright = Color(0xFF2A2A2A),
        surfaceDim = Color(0xFF1B1B1B),
        surfaceContainerLowest = Color(0xFF1B1B1B),
        surfaceContainerLow = Color(0xFF252525),
        surfaceContainer = Color(0xFF262626),
        surfaceContainerHigh = Color(0xFF262626),
        surfaceContainerHighest = Color(0xFF2A2A2A),
        outline = Color(0xFF858585),
        outlineVariant = Color(0xFF393939),
        error = Color(0xFFF0A5A5),
        onError = Color(0xFF1B1B1B),
        errorContainer = Color(0xFF252525),
        onErrorContainer = Color(0xFFF0A5A5),
    )

val LocalSurfaceColors = staticCompositionLocalOf {
    SurfaceColors(
        Color(0xFFF4F5F7),
        Color(0xFFF0F2F6),
        Color(0xFFF7F8FA),
        Color(0xFF21734C),
        Color(0xFF8A5914),
    )
}

private val AppTypography =
    Typography(
        headlineMedium =
            TextStyle(
                fontSize = 27.sp,
                lineHeight = 38.sp,
                fontWeight = FontWeight.Medium,
                letterSpacing = (-0.8).sp,
            ),
        headlineSmall =
            TextStyle(
                fontSize = 25.sp,
                lineHeight = 35.sp,
                fontWeight = FontWeight.Medium,
                letterSpacing = (-0.5).sp,
            ),
        titleLarge =
            TextStyle(fontSize = 22.sp, lineHeight = 33.sp, fontWeight = FontWeight.Medium),
        titleMedium =
            TextStyle(fontSize = 17.sp, lineHeight = 25.sp, fontWeight = FontWeight.SemiBold),
        titleSmall =
            TextStyle(fontSize = 16.sp, lineHeight = 24.sp, fontWeight = FontWeight.Medium),
        bodyLarge =
            TextStyle(fontSize = 15.sp, lineHeight = 26.sp, fontFamily = FontFamily.SansSerif),
        bodyMedium =
            TextStyle(fontSize = 14.sp, lineHeight = 23.sp, fontFamily = FontFamily.SansSerif),
        bodySmall =
            TextStyle(fontSize = 13.sp, lineHeight = 22.sp, fontFamily = FontFamily.SansSerif),
        labelLarge =
            TextStyle(fontSize = 14.sp, lineHeight = 22.sp, fontWeight = FontWeight.Medium),
        labelMedium =
            TextStyle(fontSize = 13.sp, lineHeight = 20.sp, fontWeight = FontWeight.Medium),
        labelSmall = TextStyle(fontSize = 12.sp, lineHeight = 20.sp),
    )

@Composable
fun GesangTheme(darkTheme: Boolean = isSystemInDarkTheme(), content: @Composable () -> Unit) {
    val colors =
        if (darkTheme) {
            SurfaceColors(
                Color(0xFF252525),
                Color(0xFF2A2A2A),
                Color(0xFF262626),
                Color(0xFF8BD6AC),
                Color(0xFFE6BD7A),
            )
        } else {
            SurfaceColors(
                Color(0xFFF4F5F7),
                Color(0xFFF0F2F6),
                Color(0xFFF7F8FA),
                Color(0xFF21734C),
                Color(0xFF8A5914),
            )
        }
    CompositionLocalProvider(LocalSurfaceColors provides colors) {
        MaterialTheme(
            colorScheme = if (darkTheme) DarkScheme else LightScheme,
            typography = AppTypography,
            content = content,
        )
    }
}
