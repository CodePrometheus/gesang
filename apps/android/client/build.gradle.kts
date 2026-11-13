import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.OutputDirectory

plugins {
    id("com.android.library")
}

android {
    namespace = "dev.gesang.nativeclient"
    compileSdk {
        version = release(37) { minorApiLevel = 2 }
    }
    buildToolsVersion = "37.0.0"
    defaultConfig {
        minSdk = 23
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

abstract class GenerateNativeClient : Exec() {
    @get:OutputDirectory abstract val kotlinDirectory: DirectoryProperty

    @get:OutputDirectory abstract val nativeDirectory: DirectoryProperty
}

androidComponents.onVariants { variant ->
    val taskName = "generate${variant.name.replaceFirstChar(Char::uppercaseChar)}NativeClient"
    val nativeProfile =
        if (variant.buildType == "release") "release"
        else providers.environmentVariable("GESANG_NATIVE_PROFILE").orElse("dev").get()
    val nativeAbis =
        providers.environmentVariable("GESANG_ANDROID_ABIS").orElse("arm64-v8a x86_64").get()
    val outputRoot = layout.buildDirectory.dir("generated/${variant.name}")
    val generate =
        tasks.register<GenerateNativeClient>(taskName) {
            val repositoryRoot = rootProject.projectDir.resolve("../..")
            val protocolRoot = repositoryRoot.resolve("../chopin-ai-gesang-engine/crates/protocol")
            workingDir(repositoryRoot)
            commandLine("bash", "scripts/android-native.sh")
            environment("GESANG_NATIVE_PROFILE", nativeProfile)
            environment("GESANG_ANDROID_ABIS", nativeAbis)
            environment("GESANG_NATIVE_OUTPUT", outputRoot.get().asFile.absolutePath)
            inputs.files(
                repositoryRoot.resolve("Cargo.toml"),
                repositoryRoot.resolve("Cargo.lock"),
                repositoryRoot.resolve("rust-toolchain.toml"),
                repositoryRoot.resolve("scripts/android-native.sh"),
            )
            inputs.files(
                repositoryRoot.resolve("crates/gesang-client/Cargo.toml"),
                repositoryRoot.resolve("crates/gesang-client/uniffi.toml"),
                protocolRoot.resolve("Cargo.toml"),
            )
            inputs.dir(repositoryRoot.resolve("crates/gesang-client/src"))
            inputs.dir(repositoryRoot.resolve("crates/gesang-api/src"))
            inputs.dir(protocolRoot.resolve("src"))
            inputs.dir(protocolRoot.resolve("schema"))
            inputs.property("nativeProfile", nativeProfile)
            inputs.property("nativeAbis", nativeAbis)
            kotlinDirectory.set(outputRoot.map { it.dir("kotlin") })
            nativeDirectory.set(outputRoot.map { it.dir("jniLibs") })
        }
    variant.sources.kotlin?.addGeneratedSourceDirectory(
        generate,
        GenerateNativeClient::kotlinDirectory,
    )
    variant.sources.jniLibs?.addGeneratedSourceDirectory(
        generate,
        GenerateNativeClient::nativeDirectory,
    )
}

dependencies {
    implementation("androidx.annotation:annotation:1.11.0")
    api("net.java.dev.jna:jna:5.19.1@aar")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.11.0")
}
