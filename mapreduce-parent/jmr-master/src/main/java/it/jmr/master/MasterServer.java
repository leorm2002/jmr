package it.jmr.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.JmrUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterServer.class);
    private final MasterContext ctx;
    private final Server server;
    private final MasterDashboardHttpServer dashboardServer;
    private final AtomicBoolean stopped;

    public MasterServer(int port, List<WorkerI> workers, Path rootStoragePath, Path jarStorageDirectory, Path jobStorageDirectory) {
        this.ctx = new MasterContext(port, rootStoragePath, jarStorageDirectory, jobStorageDirectory, workers);
        this.stopped = new AtomicBoolean(false);

        jarStorageDirectory.toFile().mkdirs();
        JMRLog.debug(LOGGER, "JAR storage directory: {}", jarStorageDirectory.toString());

        jobStorageDirectory.toFile().mkdirs();
        JMRLog.debug(LOGGER, "Job storage directory: {}", jobStorageDirectory.toString());

        JMRLog.debug(LOGGER, "Starting the job executor...");
        ExecutorManager.getExecutor().submit(new MasterExecutor(ctx));
        JMRLog.debug(LOGGER, "Starting the worker health monitor...");
        ExecutorManager.getExecutor().submit(new WorkerMonitor(ctx));

        JMRLog.debug(LOGGER, "Starting the gRPC server...");
        MasterResourceDistributor resourceDistributor = new MasterResourceDistributor(ctx); // Create distributor
        this.server = ServerBuilder.forPort(port) //
                // The MapReduce service that handles job execution requests
                .addService(new MapReduceServiceImpl(ctx.jarsPaths, ctx.jobsPaths, ctx.jobs, ctx.jobQueue)) //
                // Handles the JARs uploaded from the client
                .addService(new JarServiceImpl(jarStorageDirectory, ctx.jarsPaths, resourceDistributor))
                // Handles the Jobs uploaded from the client
                .addService(new JobServiceImpl(jobStorageDirectory, ctx.jobsPaths, resourceDistributor)) //
                .maxInboundMessageSize(100 * 1024 * 1024) // 100MB
                .build();
        try {
            this.dashboardServer = new MasterDashboardHttpServer(port + 1000, ctx);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start master dashboard HTTP server", e);
        }
    }

    public void start() throws IOException {
        server.start();
        dashboardServer.start();
        LOGGER.info("╔════════════════════════════════════════╗");
        LOGGER.info("║   MapReduce Master Server v2.0 (gRPC)  ║");
        LOGGER.info("╚════════════════════════════════════════╝");
        LOGGER.info("Port: {}", ctx.port);
        LOGGER.info("Dashboard:    http://localhost:{}", ctx.port + 1000);
        LOGGER.info("JAR Storage: {}", ctx.jarStorageDir.toAbsolutePath());
        LOGGER.info("\nJob Storage: {}", ctx.jobStorageDir.toAbsolutePath());
        LOGGER.info("\ngRPC server listening...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MasterServer.this.stop();
        }));
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        if (server != null) {
            server.shutdown();
        }
        if (dashboardServer != null) {
            dashboardServer.stop();
        }
        ExecutorManager.getExecutor().shutdown();
        ctx.clearRuntimeState();
        JMRLog.debug(LOGGER, "Deleting master storage...");
        JmrUtils.deleteFolder(ctx.jarStorageDir);
        JmrUtils.deleteFolder(ctx.jobStorageDir);
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
