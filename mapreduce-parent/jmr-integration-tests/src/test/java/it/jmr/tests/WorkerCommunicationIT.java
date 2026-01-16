package it.jmr.tests;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import it.jmr.common.PartitionInfo;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.jarservice.ResourceUploadedCallback;
import it.jmr.grpc.worker.*;
import it.jmr.worker.storage.InMemoryIntermediateStorage;
import it.jmr.worker.storage.IntermediateStorage;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Worker-to-Worker communication. Tests the intermediate
 * data fetch mechanism used during the Reduce phase.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkerCommunicationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCommunicationIT.class);
    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(52000);

    private List<TestWorkerNode> workers;
    private List<ManagedChannel> channels;
    private List<Path> tempDirs;

    @BeforeEach
    void setUp() throws IOException {
        workers = new ArrayList<>();
        channels = new ArrayList<>();
        tempDirs = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }

        for (TestWorkerNode worker : workers) {
            worker.server.shutdown();
        }

        for (Path tempDir : tempDirs) {
            try {
                Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                LOGGER.warn("Cleanup failed: {}", tempDir);
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Fetch intermediate data from remote worker")
    void testFetchIntermediateData_RemoteWorker() throws Exception {
        // Start two workers
        TestWorkerNode worker1 = startWorker("worker-1");
        TestWorkerNode worker2 = startWorker("worker-2");
        workers.add(worker1);
        workers.add(worker2);

        // Save intermediate data on worker1
        String taskId = "map-task-1";
        String partitionId = "partition-0";
        List<Serializable> testData = Arrays.asList("value1", "value2", "value3");
        worker1.intermediateStorage.savePartitionData(taskId, partitionId, testData);

        // Fetch data from worker1 using worker2's stub
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker1.port);

        FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder().setTaskId(taskId).setPartitionId(partitionId).build();

        // Collect all chunks
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);

        while (chunks.hasNext()) {
            IntermediateDataChunk chunk = chunks.next();
            baos.write(chunk.getData().toByteArray());
        }

        // Deserialize and verify
        List<Serializable> fetchedData = deserialize(baos.toByteArray());

        assertEquals(testData.size(), fetchedData.size(), "Fetched data should have same size as original");
        assertEquals(testData, fetchedData, "Fetched data should match original");

        LOGGER.info("Remote fetch test passed - fetched {} items", fetchedData.size());
    }

    @Test
    @Order(2)
    @DisplayName("Fetch large intermediate data with chunking")
    void testFetchIntermediateData_LargeData() throws Exception {
        TestWorkerNode worker = startWorker("worker-large");
        workers.add(worker);

        // Create large test data
        String taskId = "map-task-large";
        String partitionId = "partition-large";
        List<Serializable> largeData = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeData.add("large-value-" + i + "-" + "x".repeat(100));
        }
        worker.intermediateStorage.savePartitionData(taskId, partitionId, largeData);

        // Fetch
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker.port);
        FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder().setTaskId(taskId).setPartitionId(partitionId).build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);
        int chunkCount = 0;

        while (chunks.hasNext()) {
            IntermediateDataChunk chunk = chunks.next();
            baos.write(chunk.getData().toByteArray());
            chunkCount++;
        }

        // Verify
        List<Serializable> fetchedData = deserialize(baos.toByteArray());

        assertEquals(largeData.size(), fetchedData.size());
        assertTrue(chunkCount > 1, "Large data should be sent in multiple chunks");

        LOGGER.info("Large data fetch test passed - {} chunks, {} items", chunkCount, fetchedData.size());
    }

    @Test
    @Order(3)
    @DisplayName("Fetch data from multiple partitions")
    void testFetchIntermediateData_MultiplePartitions() throws Exception {
        TestWorkerNode worker = startWorker("worker-multi");
        workers.add(worker);

        String taskId = "map-task-multi";
        int numPartitions = 3;

        // Save data for multiple partitions
        Map<String, List<Serializable>> partitionData = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            String partitionId = "partition-" + i;
            List<Serializable> data = Arrays.asList("p" + i + "-v1", "p" + i + "-v2");
            worker.intermediateStorage.savePartitionData(taskId, partitionId, data);
            partitionData.put(partitionId, data);
        }

        // Fetch all partitions
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker.port);

        for (int i = 0; i < numPartitions; i++) {
            String partitionId = "partition-" + i;

            FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder().setTaskId(taskId).setPartitionId(partitionId).build();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);
            while (chunks.hasNext()) {
                baos.write(chunks.next().getData().toByteArray());
            }

            List<Serializable> fetched = deserialize(baos.toByteArray());
            assertEquals(partitionData.get(partitionId), fetched, "Partition " + partitionId + " data should match");
        }

        LOGGER.info("Multiple partitions fetch test passed - {} partitions", numPartitions);
    }

    @Test
    @Order(4)
    @DisplayName("Concurrent fetches from same worker")
    void testFetchIntermediateData_Concurrent() throws Exception {
        TestWorkerNode worker = startWorker("worker-concurrent");
        workers.add(worker);

        // Prepare multiple partitions
        int numPartitions = 5;
        for (int i = 0; i < numPartitions; i++) {
            String taskId = "task-" + i;
            String partitionId = "part-" + i;
            List<Serializable> data = Arrays.asList("data-" + i);
            worker.intermediateStorage.savePartitionData(taskId, partitionId, data);
        }

        // Concurrent fetches
        ExecutorService executor = Executors.newFixedThreadPool(numPartitions);
        CountDownLatch latch = new CountDownLatch(numPartitions);
        List<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < numPartitions; i++) {
            final int idx = i;
            results.add(executor.submit(() -> {
                try {
                    WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker.port);

                    FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder().setTaskId("task-" + idx)
                            .setPartitionId("part-" + idx).build();

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);
                    while (chunks.hasNext()) {
                        baos.write(chunks.next().getData().toByteArray());
                    }

                    List<Serializable> fetched = deserialize(baos.toByteArray());
                    return fetched.get(0).equals("data-" + idx);
                } finally {
                    latch.countDown();
                }
            }));
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All fetches should complete");

        for (Future<Boolean> result : results) {
            assertTrue(result.get(), "Each fetch should return correct data");
        }

        executor.shutdown();
        LOGGER.info("Concurrent fetch test passed - {} concurrent fetches", numPartitions);
    }

    @Test
    @Order(5)
    @DisplayName("Worker heartbeat communication")
    void testWorkerHeartbeat() throws Exception {
        TestWorkerNode worker = startWorker("worker-heartbeat");
        workers.add(worker);

        WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker.port);

        HeartbeatRequest request = HeartbeatRequest.newBuilder().setWorkerId("test-worker").build();

        HeartbeatResponse response = stub.heartbeat(request);

        assertTrue(response.getOk(), "Heartbeat should return OK");
        LOGGER.info("Heartbeat test passed");
    }

    @Test
    @Order(6)
    @DisplayName("Get worker status")
    void testGetWorkerStatus() throws Exception {
        TestWorkerNode worker = startWorker("worker-status");
        workers.add(worker);

        WorkerServiceGrpc.WorkerServiceBlockingStub stub = createBlockingStub(worker.port);

        GetWorkerStatusRequest request = GetWorkerStatusRequest.newBuilder().setWorkerId(worker.workerId).build();

        GetWorkerStatusResponse response = stub.getWorkerStatus(request);

        assertEquals(worker.workerId, response.getWorkerId());
        assertEquals(WorkerState.IDLE, response.getState());
        assertNotNull(response.getCurrentStatus());

        LOGGER.info("Worker status test passed - state: {}", response.getState());
    }

    // ==================== Helper classes and methods ====================

    record TestWorkerNode(String workerId, int port, Server server, IntermediateStorage intermediateStorage) {
    }

    private TestWorkerNode startWorker(String workerId) throws IOException {
        int port = PORT_COUNTER.getAndIncrement();

        Path tempDir = Files.createTempDirectory("jmr-worker-comm-" + workerId);
        tempDirs.add(tempDir);

        String jarDir = tempDir.resolve("jars").toString();
        String jobDir = tempDir.resolve("jobs").toString();
        new File(jarDir).mkdirs();
        new File(jobDir).mkdirs();

        InMemoryIntermediateStorage intermediateStorage = new InMemoryIntermediateStorage();
        ConcurrentHashMap<String, String> jarStorage = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> jobStorage = new ConcurrentHashMap<>();

        // Create a simple WorkerService implementation for testing
        TestWorkerServiceImpl workerService = new TestWorkerServiceImpl(workerId, intermediateStorage);

        ResourceUploadedCallback dummyCallback = new ResourceUploadedCallback() {
            @Override
            public void onJarUploaded(String jarId, String jarPath) {
            }

            @Override
            public void onJobUploaded(String jobId, String jobPath) {
            }
        };

        Server server = ServerBuilder.forPort(port).addService(workerService).addService(new JarServiceImpl(jarDir, jarStorage, dummyCallback))
                .addService(new JobServiceImpl(jobDir, jobStorage, dummyCallback)).maxInboundMessageSize(100 * 1024 * 1024).build().start();

        LOGGER.info("Started test worker {} on port {}", workerId, port);
        return new TestWorkerNode(workerId, port, server, intermediateStorage);
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub createBlockingStub(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        channels.add(channel);
        return WorkerServiceGrpc.newBlockingStub(channel);
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) ois.readObject();
        }
    }

    /**
     * Simplified WorkerService implementation for testing communication
     */
    static class TestWorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
        private final String workerId;
        private final IntermediateStorage intermediateStorage;

        TestWorkerServiceImpl(String workerId, IntermediateStorage intermediateStorage) {
            this.workerId = workerId;
            this.intermediateStorage = intermediateStorage;
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            responseObserver.onNext(HeartbeatResponse.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getWorkerStatus(GetWorkerStatusRequest request, StreamObserver<GetWorkerStatusResponse> responseObserver) {
            WorkerStatus status = WorkerStatus.newBuilder().setActiveTasks(0).setCpuUsage(50.0).setMemoryUsage(50.0).build();

            GetWorkerStatusResponse response = GetWorkerStatusResponse.newBuilder().setWorkerId(workerId).setState(WorkerState.IDLE)
                    .setCurrentStatus(status).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void fetchIntermediateData(FetchIntermediateDataRequest request, StreamObserver<IntermediateDataChunk> responseObserver) {
            try {
                List<Serializable> data = intermediateStorage.getPartitionData(request.getTaskId(), request.getPartitionId());

                if (data == null) {
                    responseObserver.onError(new RuntimeException("Data not found"));
                    return;
                }

                // Serialize the data
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                    oos.writeObject(data);
                    oos.flush();
                }

                byte[] serializedData = baos.toByteArray();

                // Send in chunks (64KB)
                int chunkSize = 64 * 1024;
                int offset = 0;

                while (offset < serializedData.length) {
                    int length = Math.min(chunkSize, serializedData.length - offset);
                    boolean isLast = (offset + length >= serializedData.length);

                    IntermediateDataChunk chunk = IntermediateDataChunk.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFrom(serializedData, offset, length)).setIsLast(isLast).build();

                    responseObserver.onNext(chunk);
                    offset += length;
                }

                responseObserver.onCompleted();

            } catch (IOException e) {
                responseObserver.onError(e);
            }
        }
    }
}
