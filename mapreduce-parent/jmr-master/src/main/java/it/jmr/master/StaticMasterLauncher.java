package it.jmr.master;

import java.nio.file.Path;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import it.jmr.common.exceptions.JMRException;

/**
 * Starts a master with an explicit worker list, bypassing network discovery.
 * This keeps end-to-end tests deterministic when mDNS is not available.
 */
public class StaticMasterLauncher {

    @Parameter(names = { "--port", "-p" }, required = true, description = "The port on which the master server will listen")
    private int port;

    @Parameter(names = { "--storageDirectory", "-sd" }, required = true, description = "The root directory where data will be stored")
    private String storageDirectory;

    @Parameter(names = { "--worker" }, required = true, variableArity = true, description = "Worker definition in the form workerId:host:port")
    private List<String> workerDefinitions;

    public static void main(final String[] args) throws JMRException {
        final StaticMasterLauncher app = new StaticMasterLauncher();
        JCommander.newBuilder().addObject(app).build().parse(args);

        final Path rootStoragePath = Path.of(app.storageDirectory).resolve("master-data");
        final Path jarStorageDirectory = rootStoragePath.resolve("jars");
        final Path jobStorageDirectory = rootStoragePath.resolve("jobs");
        final List<WorkerI> workers = app.workerDefinitions.stream().map(StaticMasterLauncher::parseWorker).toList();

        try (final MasterServer master = new MasterServer(app.port, workers, rootStoragePath, jarStorageDirectory, jobStorageDirectory)) {
            master.start();
            master.blockUntilShutdown();
        } catch (Exception e) {
            throw new JMRException("Error during static master startup", e);
        }
    }

    private static WorkerI parseWorker(final String workerDefinition) {
        final String[] parts = workerDefinition.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Worker definition must be workerId:host:port, got: " + workerDefinition);
        }

        return new WorkerI(parts[0], parts[1], Integer.parseInt(parts[2]));
    }
}
