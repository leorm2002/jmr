package it.jmr.common.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JmrUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;

/**
 * Keeps a discoverable service active as long as the program is running.
 */
public class DiscoverableService implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoverableService.class);
    private final String serviceName;
    private final String serviceType;
    private final int port;
    private JmDNS jmdns;
    private volatile boolean running = false;

    public DiscoverableService(String serviceName, String serviceType, int port) {
        this.serviceName = serviceName;
        this.serviceType = serviceType;
        this.port = port;
    }

    /** Starts the service in a persistent virtual thread */
    public void start() {
        if (running)
            return;
        running = true;

        ExecutorService executor = ExecutorManager.getExecutor();
        executor.submit(() -> {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                jmdns = JmDNS.create(addr);
                ServiceInfo info = ServiceInfo.create(serviceType, serviceName, port, "Running service");
                jmdns.registerService(info);
                LOGGER.info("Service '{}' registered at {}:{}", serviceName, addr.getHostAddress(), port);

                // stays alive as long as running = true
                while (running) {
                    JmrUtils.sleep(1000);
                }
            } catch (IOException e) {
                LOGGER.error("Error in DiscoverableService", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (jmdns != null) {
            jmdns.unregisterAllServices();
            jmdns.close();
            LOGGER.info("Service '{}' unregistered", serviceName);
        }
    }
}
