package it.jmr.tests;

import it.jmr.common.jarservice.JarServiceClient;
import it.jmr.grpc.JarServiceGrpc;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JarService - tests JAR upload functionality
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JarServiceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(JarServiceIT.class);

    private TestServerManager serverManager;
    private TestServerManager.TestServerInfo serverInfo;

    @BeforeEach
    void setUp() throws IOException {
        serverManager = new TestServerManager();
        serverInfo = serverManager.startServer();
        LOGGER.info("Test server started on port {}", serverInfo.port());
    }

    @AfterEach
    void tearDown() {
        if (serverManager != null) {
            serverManager.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Upload JAR file successfully")
    void testUploadJar_Success() throws Exception {
        // Create a test JAR file
        byte[] jarContent = createFakeJarContent();
        Path jarFile = serverManager.createTempFile("test", ".jar", jarContent);

        // Create client stub
        JarServiceGrpc.JarServiceStub stub = serverManager.createJarServiceStub(serverInfo.port());

        // Upload the JAR
        String jarId = JarServiceClient.uploadJar(jarFile.toString(), stub);

        // Verify
        assertNotNull(jarId, "JAR ID should not be null");
        assertFalse(jarId.isEmpty(), "JAR ID should not be empty");

        // Verify callback was invoked
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> !serverInfo.callback().getUploadedJars().isEmpty());

        assertTrue(serverInfo.callback().getUploadedJars().contains(jarId),
                "Callback should have been invoked with the JAR ID");

        // Verify JAR was stored
        assertTrue(serverInfo.jarStorage().containsKey(jarId),
                "JAR should be in storage map");

        // Verify file exists on disk
        String storedPath = serverInfo.jarStorage().get(jarId);
        assertTrue(Files.exists(Path.of(storedPath)),
                "JAR file should exist on disk");

        // Verify content matches
        byte[] storedContent = Files.readAllBytes(Path.of(storedPath));
        assertArrayEquals(jarContent, storedContent,
                "Stored JAR content should match original");

        LOGGER.info("JAR upload test passed - ID: {}", jarId);
    }

    @Test
    @Order(2)
    @DisplayName("Upload large JAR file with chunking")
    void testUploadLargeJar_Chunking() throws Exception {
        // Create a large JAR file (1MB) to test chunking
        byte[] largeContent = new byte[1024 * 1024];
        for (int i = 0; i < largeContent.length; i++) {
            largeContent[i] = (byte) (i % 256);
        }
        Path jarFile = serverManager.createTempFile("large-test", ".jar", largeContent);

        // Upload
        JarServiceGrpc.JarServiceStub stub = serverManager.createJarServiceStub(serverInfo.port());
        String jarId = JarServiceClient.uploadJar(jarFile.toString(), stub);

        // Verify
        assertNotNull(jarId);

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> serverInfo.jarStorage().containsKey(jarId));

        String storedPath = serverInfo.jarStorage().get(jarId);
        byte[] storedContent = Files.readAllBytes(Path.of(storedPath));
        assertArrayEquals(largeContent, storedContent,
                "Large JAR content should match after chunked transfer");

        LOGGER.info("Large JAR upload test passed - Size: {} bytes", largeContent.length);
    }

    @Test
    @Order(3)
    @DisplayName("Upload multiple JARs concurrently")
    void testUploadMultipleJars_Concurrent() throws Exception {
        int numJars = 3;
        String[] jarIds = new String[numJars];

        JarServiceGrpc.JarServiceStub stub = serverManager.createJarServiceStub(serverInfo.port());

        // Upload multiple JARs
        for (int i = 0; i < numJars; i++) {
            byte[] content = ("JAR content " + i).getBytes();
            Path jarFile = serverManager.createTempFile("test-" + i, ".jar", content);
            jarIds[i] = JarServiceClient.uploadJar(jarFile.toString(), stub);
        }

        // Verify all JARs were uploaded
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> serverInfo.jarStorage().size() >= numJars);

        for (String jarId : jarIds) {
            assertNotNull(jarId);
            assertTrue(serverInfo.jarStorage().containsKey(jarId),
                    "JAR " + jarId + " should be in storage");
        }

        assertEquals(numJars, serverInfo.callback().getUploadedJars().size(),
                "Callback should have been invoked for all JARs");

        LOGGER.info("Multiple JAR upload test passed - {} JARs uploaded", numJars);
    }

    @Test
    @Order(4)
    @DisplayName("Upload JAR with non-existent file throws exception")
    void testUploadJar_FileNotFound() {
        JarServiceGrpc.JarServiceStub stub = serverManager.createJarServiceStub(serverInfo.port());

        assertThrows(Exception.class, () -> {
            JarServiceClient.uploadJar("/nonexistent/path/to/file.jar", stub);
        }, "Should throw exception for non-existent file");

        LOGGER.info("File not found test passed");
    }

    /**
     * Creates fake JAR content for testing (simple byte array with JAR magic bytes)
     */
    private byte[] createFakeJarContent() {
        // JAR files start with PK (ZIP magic bytes)
        byte[] content = new byte[1000];
        content[0] = 0x50; // P
        content[1] = 0x4B; // K
        content[2] = 0x03;
        content[3] = 0x04;
        // Fill rest with test data
        for (int i = 4; i < content.length; i++) {
            content[i] = (byte) (i % 256);
        }
        return content;
    }
}
