package it.jmr.worker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import it.jmr.common.JMRConstants;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.FetchIntermediateDataRequest;
import it.jmr.grpc.worker.IntermediateDataChunk;
import it.jmr.grpc.worker.WorkerServiceGrpc;
import it.jmr.worker.models.WorkerContext;
import it.jmr.worker.storage.InMemoryIntermediateStorage;

public class WorkerServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServer.class);
    private final int port;
    private final Server server;
    final String storageDir;
    final WorkerContext ctx;
    private HealthStatusManager healthStatusManager;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, WorkerServiceGrpc.WorkerServiceBlockingStub> blockingStubs = new ConcurrentHashMap<>();

    public WorkerServer(String workerId, int port, String jarStorageDir, String jobStorageDir) {
        this.port = port;
        this.storageDir = jarStorageDir;
        this.ctx = new WorkerContext(new InMemoryIntermediateStorage(), workerId);
        this.healthStatusManager = new HealthStatusManager();

        // Create storage directory
        new File(jarStorageDir).mkdirs();

        // Create job storage directory
        new File(jobStorageDir).mkdirs();

        this.server = ServerBuilder.forPort(port)//
                .addService(new WorkerServiceImpl(ctx))//
                .addService(new JarServiceImpl(jarStorageDir, ctx.jarStorage))//
                .addService(new JobServiceImpl(jobStorageDir, ctx.jobStorage))//
                .addService(healthStatusManager.getHealthService()) // Add health service
                .maxInboundMessageSize(JMRConstants.MAX_INBOUND_MESSAGE_SIZE).build();

        // Set health status
        healthStatusManager.setStatus(JMRConstants.HEALTH_SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
    }

    public void start() throws IOException {
        server.start();
        LOGGER.info("╔════════════════════════════════════════╗");
        LOGGER.info("║            jMR   Worker Node           ║");
        LOGGER.info("╚════════════════════════════════════════╝");
        LOGGER.info("Worker ID:    {}", ctx.workerId);
        LOGGER.info("Port:         {}", port);
        LOGGER.info("Storage:      {}", new File(storageDir).getAbsolutePath());
        LOGGER.info("\nWorker ready to receive tasks...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("\nShutting down worker...");
            WorkerServer.this.stop();
        }));
    }

    public void stop() {
        if (server != null) {
            JMRLog.debug(LOGGER, "Shutting down health service...");
            healthStatusManager.enterTerminalState();
            server.shutdown();
        }
        JMRLog.debug(LOGGER, "Shutting down gRPC channels...");
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Channel shutdown interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private ManagedChannel getOrCreateChannel(String host, int port) {
        String key = host + ":" + port;
        return channels.computeIfAbsent(key, k -> {
            JMRLog.debug(LOGGER, "Creating new channel for {}:{}", host, port);
            return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        });
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub getOrCreateBlockingStub(String host, int port) {
        String key = host + ":" + port;
        return blockingStubs.computeIfAbsent(key, k -> {
            JMRLog.debug(LOGGER, "Creating new blocking stub for {}:{}", host, port);
            ManagedChannel channel = getOrCreateChannel(host, port);
            return WorkerServiceGrpc.newBlockingStub(channel);
        });
    }

    @SuppressWarnings("unchecked")
    private <KEY extends Serializable, VALUE extends Serializable> List<Pair<KEY, VALUE>> fetchIntermediateDataFromWorker(String workerId,
            String host, int port, String taskId, int partitionId) throws JMRException {

        // If it's this worker, read directly from disk
        if (workerId.equals(this.ctx.workerId)) {
            JMRLog.debug(LOGGER, "Fetching intermediate data locally for task {} partition {}", taskId, partitionId);
            String filePath = storageDir + "/map_" + taskId + "_part_" + partitionId + ".dat";
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                return (List<Pair<KEY, VALUE>>) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new JMRException("Error fetching local intermediate data", e);
            }
        }

        // Otherwise, fetch via gRPC
        JMRLog.debug(LOGGER, "Fetching intermediate data from remote worker {}:{} for task {} partition {}", host, port, taskId, partitionId);
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = getOrCreateBlockingStub(host, port);
        FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder()
                .setTaskId(taskId)
                .setPartitionId(String.valueOf(partitionId)) // partitionId is int in Java, string in proto
                .build();

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);
            while (chunks.hasNext()) {
                IntermediateDataChunk chunk = chunks.next();
                baos.write(chunk.getData().toByteArray());
            }
            return deserialize(baos.toByteArray());
        } catch (IOException | ClassNotFoundException e) {
            throw new JMRException("Error fetching remote intermediate data", e);
        }
    }

    @SuppressWarnings("unchecked")
    <T> T deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) ois.readObject();
        }
    }

}