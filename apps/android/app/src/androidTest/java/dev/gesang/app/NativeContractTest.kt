package dev.gesang.app

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.gesang.nativeclient.Client
import dev.gesang.nativeclient.ClientException
import dev.gesang.nativeclient.GrantView
import dev.gesang.nativeclient.PendingCommand
import dev.gesang.nativeclient.Permission
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Exercise the real packaged Rust library and generated ABI; these tests do not claim cloud
 * execution.
 */
@RunWith(AndroidJUnit4::class)
class NativeContractTest {
    @Test
    fun packagedSdkPreservesAnUncertainDeliveryIdentity() {
        val client = Client("https://gesang.example", null, false)
        try {
            val agent = UUID.randomUUID().toString()
            val grant =
                GrantView(
                    UUID.randomUUID().toString(),
                    agent,
                    1L,
                    Permission.FULL_ACCESS,
                    emptyList(),
                    null,
                )
            val pending = client.prepareGoal(agent, grant, "Native ABI test objective", emptyList())
            val restored = PendingCommand.restore(pending.encode())
            try {
                assertEquals(pending.requestId(), restored.requestId())
                assertEquals(agent, restored.agentId())
            } finally {
                restored.close()
                pending.close()
            }
            assertThrows(ClientException.InvalidInput::class.java) {
                client.prepareStop(agent, "not-a-canonical-goal-id", 1uL)
            }
        } finally {
            client.close()
        }
    }

    @Test
    fun packagedSdkRejectsReleaseCleartextAndCredentialBearingAddresses() {
        assertThrows(ClientException.InvalidAddress::class.java) {
            Client("http://127.0.0.1:8080", null, false)
        }
        assertThrows(ClientException.InvalidAddress::class.java) {
            Client("https://user:secret@gesang.example", null, false)
        }
        Client("http://10.0.2.2:8080", null, true).close()
    }
}
