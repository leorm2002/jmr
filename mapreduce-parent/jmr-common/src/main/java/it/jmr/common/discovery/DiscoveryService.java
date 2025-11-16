package it.jmr.common.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JmrUtils;

import java.io.IOException;
import java.net.InetAddress;

import javax.jmdns.ServiceInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DiscoveryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryService.class);
    private final String serviceType;

    public DiscoveryService(String serviceType) {
        this.serviceType = serviceType;
    }

    public List<ServiceInfo> discover(int seconds) throws IOException {
        InetAddress addr = InetAddress.getLocalHost();
        JmDNS jmdns = JmDNS.create(addr);
        List<ServiceInfo> found = Collections.synchronizedList(new ArrayList<>());

        jmdns.addServiceListener(serviceType, new ServiceListener() {
            @Override
            public void serviceAdded(ServiceEvent event) {
                LOGGER.debug("Service added: {}", event.getName());
                jmdns.requestServiceInfo(serviceType, event.getName());
            }

            @Override
            public void serviceRemoved(ServiceEvent event) {
                LOGGER.info("Service removed: {}", event.getName());
            }

            @Override
            public void serviceResolved(ServiceEvent event) {
                ServiceInfo info = event.getInfo();
                found.add(info);
                LOGGER.info("Service resolved: {} -> {}:{}", info.getName(), info.getInetAddresses()[0].getHostAddress(), info.getPort());
            }
        });

        LOGGER.info("Discovering for {} seconds...", seconds);

        // wait on a virtual thread
        CompletableFuture<Void> waitFuture = CompletableFuture.runAsync(() -> {
            JmrUtils.sleep(seconds * 1000);

        }, ExecutorManager.getExecutor());

        waitFuture.join(); // blocking for the caller

        jmdns.close();
        LOGGER.info("Discovery finished. Found {} services.", found.size());
        return new ArrayList<>(found);
    }
}
