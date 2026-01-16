package it.jmr.master;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import javax.jmdns.ServiceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import it.jmr.common.discovery.DiscoveryService;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JMRLog;

public class MasterLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterLauncher.class);

    @Parameter(names = { "--port", "-p" }, required = true, description = "The port on which the master server will listen")
    private int port;
    @Parameter(names = { "--storageDirectory", "-sd" }, required = true, description = "The root directory where data will be stored")
    private String storageDirectory;

    @Parameter(names = { "--timeoutSeconds", "-ts" }, description = "The timeout in seconds for network operations (default: 5 seconds)")
    private int timeoutSeconds = 5;

    public static void main(String[] args) throws JMRException {

        // 1. Parse command line arguments
        final MasterLauncher app = new MasterLauncher();
        JCommander.newBuilder().addObject(app).build().parse(args);

        Objects.requireNonNull(app.storageDirectory, "Storage directory must be specified");
        Objects.requireNonNull(app.port, "Port must be specified");

        final Path rootStoragePath = Path.of(app.storageDirectory).resolve("master-data");

        final int port = app.port;
        final Path jarStorageDirectory = rootStoragePath.resolve("jars");
        final Path jobStorageDirectory = rootStoragePath.resolve("jobs");

        // 2. Search for workers on the network
        LOGGER.info("Discovering workers on the network...");
        try {
            final DiscoveryService discovery = new DiscoveryService("_jmr._tcp.local.");
            final List<ServiceInfo> found = discovery.discover(app.timeoutSeconds);

            // 3. Start the master server

            final List<WorkerI> workers = found.stream()
                    .collect(java.util.stream.Collectors.toMap(ServiceInfo::getName,
                            serviceInfo -> new WorkerI(serviceInfo.getName(), serviceInfo.getHostAddresses()[0], serviceInfo.getPort()),
                            (existing, replacement) -> existing // keep the first occurrence
                    )).values().stream().toList();

            if (workers.isEmpty()) {
                JMRLog.warn(LOGGER, "No workers found on the network. The master will start with no workers.");
                System.exit(0);

            } else {
                JMRLog.info(LOGGER, "Discovered workers:");
                for (WorkerI worker : workers) {
                    JMRLog.info(LOGGER, " - {} at {}:{}", worker.workerId(), worker.address(), worker.port());
                }
            }

            try (MasterServer master = new MasterServer(port, workers, rootStoragePath, jarStorageDirectory, jobStorageDirectory)) {
                master.start();
                master.blockUntilShutdown();
            }
        } catch (Exception e) {
            throw new JMRException("Error during master startup", e);
        }
    }

}
