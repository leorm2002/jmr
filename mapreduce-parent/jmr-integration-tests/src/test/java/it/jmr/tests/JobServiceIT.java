package it.jmr.tests;

import it.jmr.common.jarservice.JobServiceClient;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.JobServiceGrpc;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JobService - tests Job upload functionality
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JobServiceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceIT.class);

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
    @DisplayName("Upload Job configuration successfully")
    void testUploadJob_Success() throws Exception {
        // Create a test job configuration
        TestJobConfiguration jobConfig = new TestJobConfiguration("test-job-1");

        // Create client stub
        JobServiceGrpc.JobServiceStub stub = serverManager.createJobServiceStub(serverInfo.port());

        // Upload the job
        JobServiceClient.JobUploadResult result = JobServiceClient.serializeAndUploadJob(jobConfig, stub);

        // Verify
        assertNotNull(result, "Result should not be null");
        assertNotNull(result.getJobId(), "Job ID should not be null");
        assertFalse(result.getJobId().isEmpty(), "Job ID should not be empty");

        // Verify callback was invoked
        await().atMost(5, TimeUnit.SECONDS).until(() -> !serverInfo.callback().getUploadedJobs().isEmpty());

        assertTrue(serverInfo.callback().getUploadedJobs().contains(result.getJobId()), "Callback should have been invoked with the Job ID");

        // Verify job was stored
        assertTrue(serverInfo.jobStorage().containsKey(result.getJobId()), "Job should be in storage map");

        // Verify file exists on disk
        String storedPath = serverInfo.jobStorage().get(result.getJobId());
        assertTrue(Files.exists(Path.of(storedPath)), "Job file should exist on disk");

        LOGGER.info("Job upload test passed - ID: {}", result.getJobId());
    }

    @Test
    @Order(2)
    @DisplayName("Upload Job from file successfully")
    void testUploadJobFromFile_Success() throws Exception {
        // Create and serialize a job configuration to file
        TestJobConfiguration jobConfig = new TestJobConfiguration("test-job-from-file");
        byte[] serialized = serializeObject(jobConfig);
        Path jobFile = serverManager.createTempFile("job", ".ser", serialized);

        // Create client stub
        JobServiceGrpc.JobServiceStub stub = serverManager.createJobServiceStub(serverInfo.port());

        // Upload the job from file
        JobServiceClient.JobUploadResult result = JobServiceClient.uploadJobFromFile(jobFile.toString(), "AAAA", stub);

        // Verify
        assertNotNull(result);
        assertNotNull(result.getJobId());

        await().atMost(5, TimeUnit.SECONDS).until(() -> serverInfo.jobStorage().containsKey(result.getJobId()));

        // Verify file content matches
        String storedPath = serverInfo.jobStorage().get(result.getJobId());
        byte[] storedContent = Files.readAllBytes(Path.of(storedPath));
        assertArrayEquals(serialized, storedContent, "Stored job content should match original serialized content");

        LOGGER.info("Job upload from file test passed - ID: {}", result.getJobId());
    }

    @Test
    @Order(3)
    @DisplayName("Upload large Job with chunking")
    void testUploadLargeJob_Chunking() throws Exception {
        // Create a job with large data
        LargeTestJobConfiguration largeJobConfig = new LargeTestJobConfiguration("large-job", 100000);

        JobServiceGrpc.JobServiceStub stub = serverManager.createJobServiceStub(serverInfo.port());
        JobServiceClient.JobUploadResult result = JobServiceClient.serializeAndUploadJob(largeJobConfig, stub);

        // Verify
        assertNotNull(result);
        assertNotNull(result.getJobId());

        await().atMost(10, TimeUnit.SECONDS).until(() -> serverInfo.jobStorage().containsKey(result.getJobId()));

        LOGGER.info("Large job upload test passed - ID: {}", result.getJobId());
    }

    @Test
    @Order(4)
    @DisplayName("Upload multiple Jobs")
    void testUploadMultipleJobs() throws Exception {
        int numJobs = 3;
        String[] jobIds = new String[numJobs];

        JobServiceGrpc.JobServiceStub stub = serverManager.createJobServiceStub(serverInfo.port());

        // Upload multiple jobs
        for (int i = 0; i < numJobs; i++) {
            TestJobConfiguration jobConfig = new TestJobConfiguration("multi-job-" + i);
            JobServiceClient.JobUploadResult result = JobServiceClient.serializeAndUploadJob(jobConfig, stub);
            jobIds[i] = result.getJobId();
        }

        // Verify all jobs were uploaded
        await().atMost(10, TimeUnit.SECONDS).until(() -> serverInfo.jobStorage().size() >= numJobs);

        for (String jobId : jobIds) {
            assertNotNull(jobId);
            assertTrue(serverInfo.jobStorage().containsKey(jobId), "Job " + jobId + " should be in storage");
        }

        assertEquals(numJobs, serverInfo.callback().getUploadedJobs().size(), "Callback should have been invoked for all jobs");

        LOGGER.info("Multiple job upload test passed - {} jobs uploaded", numJobs);
    }

    @Test
    @Order(5)
    @DisplayName("Upload Job from non-existent file throws exception")
    void testUploadJobFromFile_FileNotFound() {
        JobServiceGrpc.JobServiceStub stub = serverManager.createJobServiceStub(serverInfo.port());

        assertThrows(Exception.class, () -> {
            JobServiceClient.uploadJobFromFile("/nonexistent/path/to/job.ser", "NON", stub);
        }, "Should throw exception for non-existent file");

        LOGGER.info("File not found test passed");
    }

    private byte[] serializeObject(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    /**
     * Simple test job configuration for testing
     */
    static class TestJobConfiguration implements JobConfiguration<String, String, String> {
        private static final long serialVersionUID = 1L;
        private final String name;

        public TestJobConfiguration(String name) {
            this.name = name;
        }

        @Override
        public DataProviderClient<String> getDataProvider() {
            return null; // Not needed for upload tests
        }

        @Override
        public SerializableMapper<String, String> getMapper() {
            return data -> Collections.singletonList(new Pair<>("key", data));
        }

        @Override
        public SerializableReducer<String, String> getReducer() {
            return (key, values) -> new Pair<>(key, values.isEmpty() ? "" : values.get(0));
        }
    }

    /**
     * Large test job configuration to test chunking
     */
    static class LargeTestJobConfiguration implements JobConfiguration<String, String, String> {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final byte[] largeData;

        public LargeTestJobConfiguration(String name, int dataSize) {
            this.name = name;
            this.largeData = new byte[dataSize];
            for (int i = 0; i < dataSize; i++) {
                largeData[i] = (byte) (i % 256);
            }
        }

        @Override
        public DataProviderClient<String> getDataProvider() {
            return null;
        }

        @Override
        public SerializableMapper<String, String> getMapper() {
            return data -> Collections.singletonList(new Pair<>("key", data));
        }

        @Override
        public SerializableReducer<String, String> getReducer() {
            return (key, values) -> new Pair<>(key, values.isEmpty() ? "" : values.get(0));
        }
    }
}
