package dev.gesang.app.platform

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import dev.gesang.nativeclient.ServerConfiguration
import dev.gesang.nativeclient.TokenSet
import java.security.KeyStore
import java.security.MessageDigest
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import org.json.JSONObject

internal data class Account(
    val endpoint: String,
    val tokens: TokenSet,
    val configuration: ServerConfiguration,
    val ownerId: String? = null,
    val agentId: String? = null,
    val synchronizedAtMs: Long? = null,
)

internal class StoredAuthorization(val endpoint: String, val encoded: ByteArray)

internal fun accountRecordKey(endpoint: String?, ownerId: String?, record: String): String {
    val identity = "$endpoint\u0000$ownerId\u0000$record"
    return MessageDigest.getInstance("SHA-256")
        .digest(identity.toByteArray(Charsets.UTF_8))
        .joinToString("") { "%02x".format(it) }
}

/** Device-only encrypted records; no credential is included in system backup. */
internal class SecureStore(context: Context) {
    companion object {
        private val keyLock = Any()
    }

    private val preferences = context.getSharedPreferences("device-v1", Context.MODE_PRIVATE)
    private val keyStore = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
    private val alias = "gesang.device.v1"

    private fun key(): SecretKey =
        synchronized(keyLock) {
            val existing = keyStore.getKey(alias, null)
            if (existing is SecretKey) return@synchronized existing
            KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore")
                .apply {
                    init(
                        KeyGenParameterSpec.Builder(
                                alias,
                                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
                            )
                            .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                            .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                            .setRandomizedEncryptionRequired(true)
                            .build()
                    )
                }
                .generateKey()
        }

    fun read(name: String): ByteArray? {
        val stored = preferences.getString(name, null) ?: return null
        return decryptRecord(name, stored.toByteArray(Charsets.UTF_8))
    }

    fun decryptRecord(name: String, encoded: ByteArray): ByteArray {
        val envelope = JSONObject(encoded.toString(Charsets.UTF_8))
        check(envelope.getInt("version") == 1) { "Unsupported encrypted record version." }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        val iv = Base64.decode(envelope.getString("iv"), Base64.NO_WRAP)
        cipher.init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(128, iv))
        cipher.updateAAD(name.toByteArray(Charsets.UTF_8))
        return cipher.doFinal(Base64.decode(envelope.getString("content"), Base64.NO_WRAP))
    }

    fun write(name: String, value: ByteArray) {
        val encoded = encryptRecord(name, value)
        check(preferences.edit().putString(name, encoded.toString(Charsets.UTF_8)).commit()) {
            "Secure record could not be persisted."
        }
    }

    fun encryptRecord(name: String, value: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, key())
        cipher.updateAAD(name.toByteArray(Charsets.UTF_8))
        val content = cipher.doFinal(value)
        val envelope =
            JSONObject()
                .put("version", 1)
                .put("iv", Base64.encodeToString(cipher.iv, Base64.NO_WRAP))
                .put("content", Base64.encodeToString(content, Base64.NO_WRAP))
        return envelope.toString().toByteArray(Charsets.UTF_8)
    }

    fun clear(name: String) {
        check(preferences.edit().remove(name).commit()) { "Secure record could not be removed." }
    }

    fun saveAuthorization(endpoint: String, encoded: ByteArray) {
        val record =
            JSONObject()
                .put("version", 1)
                .put("endpoint", endpoint)
                .put("attempt", Base64.encodeToString(encoded, Base64.NO_WRAP))
        write("authorization", record.toString().toByteArray(Charsets.UTF_8))
    }

    fun authorization(): StoredAuthorization? {
        val bytes = read("authorization") ?: return null
        val record = JSONObject(bytes.toString(Charsets.UTF_8))
        check(record.getInt("version") == 1) { "Unsupported stored authorization version." }
        return StoredAuthorization(
            record.getString("endpoint"),
            Base64.decode(record.getString("attempt"), Base64.NO_WRAP),
        )
    }

    fun account(): Account? {
        val bytes = read("account") ?: return null
        val data = JSONObject(bytes.toString(Charsets.UTF_8))
        val config = data.getJSONObject("registration")
        val scopes = config.getJSONArray("scopes")
        return Account(
            endpoint = data.getString("endpoint"),
            tokens =
                TokenSet(
                    accessToken = data.getString("access"),
                    refreshToken = data.optionalString("refresh"),
                    expiresAtMs = if (data.isNull("expires")) null else data.getLong("expires"),
                ),
            configuration =
                ServerConfiguration(
                    apiVersion = config.getInt("version").toUInt(),
                    issuer = config.getString("issuer"),
                    authorizationEndpoint = config.getString("authorize"),
                    tokenEndpoint = config.getString("token"),
                    clientId = config.getString("client"),
                    redirectUri = config.getString("redirect"),
                    scopes = List(scopes.length()) { scopes.getString(it) },
                ),
            ownerId = data.optionalString("owner"),
            agentId = data.optionalString("agent"),
            synchronizedAtMs =
                if (data.isNull("synchronized")) null else data.getLong("synchronized"),
        )
    }

    fun saveAccount(account: Account) {
        val config = account.configuration
        val registration =
            JSONObject()
                .put("version", config.apiVersion.toInt())
                .put("issuer", config.issuer)
                .put("authorize", config.authorizationEndpoint)
                .put("token", config.tokenEndpoint)
                .put("client", config.clientId)
                .put("redirect", config.redirectUri)
                .put("scopes", org.json.JSONArray(config.scopes))
        val data =
            JSONObject()
                .put("endpoint", account.endpoint)
                .put("access", account.tokens.accessToken)
                .put("refresh", account.tokens.refreshToken ?: JSONObject.NULL)
                .put("expires", account.tokens.expiresAtMs ?: JSONObject.NULL)
                .put("owner", account.ownerId ?: JSONObject.NULL)
                .put("agent", account.agentId ?: JSONObject.NULL)
                .put("synchronized", account.synchronizedAtMs ?: JSONObject.NULL)
                .put("registration", registration)
        write("account", data.toString().toByteArray(Charsets.UTF_8))
    }

    private fun JSONObject.optionalString(name: String): String? =
        if (isNull(name)) null else getString(name)
}
