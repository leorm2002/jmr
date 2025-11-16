package it.jmr.worker;

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.discovery.DiscoverableService;
import it.jmr.common.exceptions.JMRException;

public class WorkerLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerLauncher.class);
    public static final ExecutorService executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    public static void main(String[] args) throws JMRException {
        if (args.length < 2) {
            LOGGER.error("Usage: WorkerNode <worker-id> <port>");
            LOGGER.error("Example: WorkerNode worker1 8081");
            return;
        }

        final String workerId = args[0];
        final int port = Integer.parseInt(args[1]);

        try {
            // Start service discovery and publishing
            LOGGER.info("Starting service discovery for worker {}", workerId);
            final DiscoverableService service = new DiscoverableService(workerId, "_jmr._tcp.local.", port);
            service.start();

            // Start worker server
            LOGGER.info("Starting worker server for worker {}", workerId);
            final String jarStorageDir = "./worker-data/" + workerId + "/jars/";
            final String jobStorageDir = "./worker-data/" + workerId + "/jobs/";
            final WorkerServer worker = new WorkerServer(workerId, port, jarStorageDir, jobStorageDir);
            worker.start();
            worker.blockUntilShutdown();
        } catch (Exception e) {
            throw new JMRException("Error during worker startup", e);
        }
    }

}
