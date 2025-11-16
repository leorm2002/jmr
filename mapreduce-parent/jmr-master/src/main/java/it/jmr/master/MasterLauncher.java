package it.jmr.master;

import java.util.List;
import java.util.Objects;

import javax.jmdns.ServiceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import it.jmr.common.discovery.DiscoveryService;
import it.jmr.common.utils.JMRLog;

public class MasterLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterLauncher.class);

    @Parameter(names = { "--port", "-p" }, required = true, description = "The port on which the master server will listen")
    private int port;

    @Parameter(names = { "--jarStorageDirectory", "-jasd" }, required = true, description = "The directory where job jars will be stored")
    private String jarStorageDirectory;

    @Parameter(names = { "--jobStorageDirectory", "-josd" }, required = true, description = "The directory where job files will be stored")
    private String jobStorageDirectory;

    public static void main(String[] args) throws Exception {

        // 1. Parse command line arguments
        final MasterLauncher app = new MasterLauncher();
        JCommander.newBuilder().addObject(app).build().parse(args);

        Objects.requireNonNull(app.jarStorageDirectory, "Jar storage directory must be specified");
        Objects.requireNonNull(app.port, "Port must be specified");
        Objects.requireNonNull(app.jobStorageDirectory, "Job storage directory must be specified");

        final int port = app.port;
        final String jarStorageDirectory = app.jarStorageDirectory;
        final String jobStorageDirectory = app.jobStorageDirectory;

        // 2. Search for workers on the network
        DiscoveryService discovery = new DiscoveryService("_jmr._tcp.local.");
        List<ServiceInfo> found = discovery.discover(2);

        // 3. Start the master server

        List<WorkerI> workers = found.stream()
                .collect(java.util.stream.Collectors.toMap(ServiceInfo::getName,
                        serviceInfo -> new WorkerI(serviceInfo.getName(), serviceInfo.getHostAddresses()[0], serviceInfo.getPort()),
                        (existing, replacement) -> existing // keep the first occurrence
                )).values().stream().toList();

        if (workers.isEmpty()) {
            JMRLog.debug(LOGGER, "No workers found on the network. The master will start with no workers.");
            System.exit(0);
        } else {
            JMRLog.debug(LOGGER, "Discovered workers:");
            for (WorkerI worker : workers) {
                JMRLog.debug(LOGGER, " - {} at {}:{}", worker.workerId(), worker.address(), worker.port());
            }
        }

        MapReduceMasterServer master = new MapReduceMasterServer(port, workers, jarStorageDirectory, jobStorageDirectory);
        master.start();
        master.blockUntilShutdown();
    }

}
