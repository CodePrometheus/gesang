-keep class com.sun.jna.** { *; }
-keepclasseswithmembernames class * { native <methods>; }
-keep class dev.gesang.nativeclient.** { *; }
# JNA documents these desktop-only types as optional on Android.
# https://github.com/java-native-access/jna/blob/5.19.1/www/FrequentlyAskedQuestions.md#jna-on-android
-dontwarn java.awt.Component
-dontwarn java.awt.GraphicsEnvironment
-dontwarn java.awt.HeadlessException
-dontwarn java.awt.Window
