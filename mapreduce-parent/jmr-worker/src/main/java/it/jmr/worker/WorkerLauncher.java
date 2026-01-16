package it.jmr.worker;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import it.jmr.common.discovery.DiscoverableService;
import it.jmr.common.exceptions.JMRException;

public class WorkerLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerLauncher.class);
    public static final ExecutorService executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    @Parameter(names = { "--workerId", "-w" }, required = true, description = "The ID of the worker")
    private String workerId;

    @Parameter(names = { "--port", "-p" }, required = true, description = "The port on which the master server will listen")
    private int port;

    @Parameter(names = { "--storageDirectory", "-sd" }, required = true, description = "The root directory where data will be stored")
    private String storageDirectory;

    public static void main(String[] args) throws JMRException {

        // 1. Parse command line arguments
        final WorkerLauncher app = new WorkerLauncher();
        JCommander.newBuilder().addObject(app).build().parse(args);

        Objects.requireNonNull(app.storageDirectory, "Storage directory must be specified");
        Objects.requireNonNull(app.port, "Port must be specified");

        final Path rootStoragePath = Path.of(app.storageDirectory).resolve("worker-data");

        final String workerId = app.workerId;
        final int port = app.port;

        try (final DiscoverableService service = new DiscoverableService(workerId, "_jmr._tcp.local.", port)) {
            // Start service discovery and publishing
            LOGGER.info("Starting service discovery for worker {}", workerId);
            service.start();

            // Start worker server
            LOGGER.info("Starting worker server for worker {}", workerId);
            final Path jarStorageDir = rootStoragePath.resolve(workerId).resolve("jars");
            final Path jobStorageDir = rootStoragePath.resolve(workerId).resolve("jobs");
            final WorkerServer worker = new WorkerServer(workerId, port, jarStorageDir, jobStorageDir);
            worker.start();
            worker.blockUntilShutdown();
        } catch (Exception e) {
            throw new JMRException("Error during worker startup", e);
        }
    }

}
