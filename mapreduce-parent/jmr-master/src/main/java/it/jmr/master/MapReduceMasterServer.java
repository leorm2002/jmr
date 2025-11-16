package it.jmr.master;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JMRLog;

import java.io.*;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceMasterServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceMasterServer.class);
    private final MasterContext ctx;
    private final Server server;

    public MapReduceMasterServer(int port, List<WorkerI> workers, String jarStorageDir, String jobStorageDir) {
        this.ctx = new MasterContext(port, jarStorageDir, jobStorageDir, workers);

        new File(jarStorageDir).mkdirs();
        JMRLog.debug(LOGGER, "JAR storage directory: {}", jarStorageDir);

        new File(jobStorageDir).mkdirs();
        JMRLog.debug(LOGGER, "Job storage directory: {}", jobStorageDir);

        JMRLog.debug(LOGGER, "Starting the job executor...");
        ExecutorManager.getExecutor().submit(new MasterExecutor(ctx));
        JMRLog.debug(LOGGER, "Starting the worker health monitor...");
        ExecutorManager.getExecutor().submit(new WorkerMonitor(ctx));

        JMRLog.debug(LOGGER, "Starting the gRPC server...");
        this.server = ServerBuilder.forPort(port) //
                .addService(new MapReduceServiceImpl(ctx.jarsPaths, ctx.jobsPaths, ctx.jobQueue)) //
                .addService(new JarServiceImpl(jarStorageDir, ctx.jarsPaths)) //
                .addService(new JobServiceImpl(jobStorageDir, ctx.jobsPaths)) //
                .maxInboundMessageSize(100 * 1024 * 1024) // 100MB
                .build();
    }

    public void start() throws IOException {
        server.start();
        LOGGER.info("╔════════════════════════════════════════╗");
        LOGGER.info("║   MapReduce Master Server v2.0 (gRPC)  ║");
        LOGGER.info("╚════════════════════════════════════════╝");
        LOGGER.info("Port: {}", ctx.port);
        LOGGER.info("JAR Storage: {}", new File(ctx.jarStorageDir).getAbsolutePath());
        LOGGER.info("\ngRPC server listening...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("\nShutting down server...");
            MapReduceMasterServer.this.stop();
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        ExecutorManager.getExecutor().shutdown();
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
