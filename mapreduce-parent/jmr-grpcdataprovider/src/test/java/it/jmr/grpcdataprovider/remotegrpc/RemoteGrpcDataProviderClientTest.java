package it.jmr.grpcdataprovider.remotegrpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;

class RemoteGrpcDataProviderClientTest {

    @Test
    void initCreatesUsableGrpcStateAndCloseResetsIt() throws Exception {
        final RemoteGrpcDataProviderClient<String> client = new RemoteGrpcDataProviderClient<>("localhost", 65535);

        assertDoesNotThrow(client::init);

        final Field channelField = RemoteGrpcDataProviderClient.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        assertNotNull(channelField.get(client));

        final Field blockingStubField = RemoteGrpcDataProviderClient.class.getDeclaredField("blockingStub");
        blockingStubField.setAccessible(true);
        assertNotNull(blockingStubField.get(client));

        client.close();

        assertNull(channelField.get(client));
        assertNull(blockingStubField.get(client));
    }
}
