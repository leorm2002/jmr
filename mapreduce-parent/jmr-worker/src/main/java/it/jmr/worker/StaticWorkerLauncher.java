package it.jmr.worker;

import java.nio.file.Path;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import it.jmr.common.exceptions.JMRException;

/**
 * Starts a worker without mDNS discovery. Useful for deterministic local and
 * CI end-to-end runs.
 */
public class StaticWorkerLauncher {

    @Parameter(names = { "--workerId", "-w" }, required = true, description = "The ID of the worker")
    private String workerId;

    @Parameter(names = { "--port", "-p" }, required = true, description = "The port on which the worker server will listen")
    private int port;

    @Parameter(names = { "--storageDirectory", "-sd" }, required = true, description = "The root directory where data will be stored")
    private String storageDirectory;

    public static void main(final String[] args) throws JMRException {
        final StaticWorkerLauncher app = new StaticWorkerLauncher();
        JCommander.newBuilder().addObject(app).build().parse(args);

        final Path rootStoragePath = Path.of(app.storageDirectory).resolve("worker-data");
        final Path jarStorageDir = rootStoragePath.resolve(app.workerId).resolve("jars");
        final Path jobStorageDir = rootStoragePath.resolve(app.workerId).resolve("jobs");

        try {
            final WorkerServer worker = new WorkerServer(app.workerId, app.port, jarStorageDir, jobStorageDir);
            worker.start();
            worker.blockUntilShutdown();
        } catch (Exception e) {
            throw new JMRException("Error during static worker startup", e);
        }
    }
}
