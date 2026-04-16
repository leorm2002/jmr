package it.jmr.worker;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import it.jmr.common.jarservice.ResourceUploadedCallback;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.JmrUtils;
import it.jmr.grpc.worker.FetchIntermediateDataRequest;
import it.jmr.grpc.worker.IntermediateDataChunk;
import it.jmr.grpc.worker.WorkerServiceGrpc;
import it.jmr.worker.models.WorkerContext;
import it.jmr.worker.storage.InMemoryIntermediateStorage;

import it.jmr.common.IntermediateDataFetcher;

public class WorkerServer implements IntermediateDataFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServer.class);
    private final Server server;
    private final WorkerDashboardHttpServer dashboardServer;
    final WorkerContext ctx;
    private HealthStatusManager healthStatusManager;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, WorkerServiceGrpc.WorkerServiceBlockingStub> blockingStubs = new ConcurrentHashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final ResourceUploadedCallback dummyCallback = new ResourceUploadedCallback() {
        @Override
        public void onJarUploaded(String jarId, Path jarPath) {
            // Do nothing on the worker side
        }

        @Override
        public void onJobUploaded(String jobId, Path jobPath) {
            // Do nothing on the worker side
        }
    };

    public WorkerServer(String workerId, int port, Path jarStorageDir, Path jobStorageDir) {

        // Create jar/job directory
        jarStorageDir.toFile().mkdirs();
        jobStorageDir.toFile().mkdirs();

        this.ctx = new WorkerContext(new InMemoryIntermediateStorage(), workerId, jarStorageDir, jobStorageDir, port);
        this.healthStatusManager = new HealthStatusManager();

        // Add log appender for dashboard
        org.apache.logging.log4j.core.LoggerContext lc = (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = lc.getConfiguration();
        org.apache.logging.log4j.core.layout.PatternLayout layout = org.apache.logging.log4j.core.layout.PatternLayout.newBuilder().withPattern("%d{HH:mm:ss} %-5level %logger{15} - %msg").build();
        org.apache.logging.log4j.core.Appender appender = new org.apache.logging.log4j.core.appender.AbstractAppender("DashboardAppender-" + System.currentTimeMillis(), null, layout, false, org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
            @Override
            public void append(org.apache.logging.log4j.core.LogEvent event) {
                String message = new String(getLayout().toByteArray(event)).trim();
                ctx.recordLog(message);
            }
        };
        appender.start();
        config.addAppender(appender);
        lc.getRootLogger().addAppender(config.getAppender(appender.getName()));
        lc.updateLoggers();

        this.server = ServerBuilder.forPort(port)//
                .addService(new WorkerServiceImpl(ctx, this))
                // Handles the uploads of JARs to this worker from the master
                .addService(new JarServiceImpl(jarStorageDir, ctx.jarStorage, dummyCallback))
                // Handles the uploads of JOBs to this worker from the master
                .addService(new JobServiceImpl(jobStorageDir, ctx.jobStorage, dummyCallback))//
                .addService(healthStatusManager.getHealthService()) // Add health service
                .maxInboundMessageSize(JMRConstants.MAX_INBOUND_MESSAGE_SIZE).build();
        try {
            this.dashboardServer = new WorkerDashboardHttpServer(port + 1000, ctx);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to start worker dashboard HTTP server", e);
        }

        // Set health status
        healthStatusManager.setStatus(JMRConstants.HEALTH_SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
    }

    public void start() throws IOException {
        server.start();
        dashboardServer.start();
        LOGGER.info("╔════════════════════════════════════════╗");
        LOGGER.info("║            jMR   Worker Node           ║");
        LOGGER.info("╚════════════════════════════════════════╝");
        LOGGER.info("Worker ID:    {}", ctx.workerId);
        LOGGER.info("Port:         {}", ctx.port);
        LOGGER.info("Dashboard:    http://localhost:{}", ctx.port + 1000);
        LOGGER.info("Jar Storage:  {}", ctx.jarStorageDir.toAbsolutePath().toString());
        LOGGER.info("Job Storage:  {}", ctx.jobStorageDir.toAbsolutePath().toString());
        LOGGER.info("\nWorker ready to receive tasks...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            WorkerServer.this.stop();
        }));
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        if (server != null) {
            JMRLog.debug(LOGGER, "Shutting down health service...");
            healthStatusManager.enterTerminalState();
            server.shutdown();
        }
        if (dashboardServer != null) {
            dashboardServer.stop();
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

        ctx.clearRuntimeState();
        JMRLog.debug(LOGGER, "Deleting worker storage...");
        JmrUtils.deleteFolder(ctx.jarStorageDir);
        JmrUtils.deleteFolder(ctx.jobStorageDir);
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

    @Override
    @SuppressWarnings("unchecked")
    public <VALUE extends Serializable> List<VALUE> fetchIntermediateData(String workerId, String host, int port, String taskId, String partitionId)
            throws JMRException {

        // If it's this worker, read directly from disk
        if (workerId.equals(this.ctx.workerId)) {
            JMRLog.debug(LOGGER, "Fetching intermediate data locally for task {} partition {}", taskId, partitionId);
            return (List<VALUE>) (List<?>) this.ctx.intermediateStorage.getPartitionData(taskId, partitionId);
        }

        // Otherwise, fetch via gRPC
        JMRLog.debug(LOGGER, "Fetching intermediate data from remote worker {}:{} for task {} partition {}", host, port, taskId, partitionId);
        WorkerServiceGrpc.WorkerServiceBlockingStub stub = getOrCreateBlockingStub(host, port);
        FetchIntermediateDataRequest request = FetchIntermediateDataRequest.newBuilder().setTaskId(taskId).setPartitionId(partitionId).build();

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            Iterator<IntermediateDataChunk> chunks = stub.fetchIntermediateData(request);
            while (chunks.hasNext()) {
                IntermediateDataChunk chunk = chunks.next();
                baos.write(chunk.getData().toByteArray());
            }
            return JmrUtils.deserialize(JmrUtils.gunzip(baos.toByteArray()));
        } catch (IOException | ClassNotFoundException e) {
            throw new JMRException("Error fetching remote intermediate data", e);
        }
    }

}
