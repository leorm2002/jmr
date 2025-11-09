package it.jmr.common.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import it.jmr.common.utils.ExecutorManager;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;


/**
 * Mantiene un servizio discoverable attivo finché il programma vive.
 */
public class DiscoverableService implements AutoCloseable {
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

    /** Avvia il servizio in un virtual thread (persistente) */
    public void start() {
        if (running) return;
        running = true;

        ExecutorService executor = ExecutorManager.getExecutor();
        executor.submit(() -> {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                jmdns = JmDNS.create(addr);
                ServiceInfo info = ServiceInfo.create(serviceType, serviceName, port, "Running service");
                jmdns.registerService(info);
                System.out.printf("Servizio '%s' registrato su %s:%d%n",
                        serviceName, addr.getHostAddress(), port);

                // rimane vivo finché running = true
                while (running) Thread.sleep(1000);
            } catch (IOException | InterruptedException e) {
                System.err.println("Errore in DiscoverableService: " + e.getMessage());
            }
        });
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (jmdns != null) {
            jmdns.unregisterAllServices();
            jmdns.close();
            System.out.printf("Servizio '%s' deregistrato%n", serviceName);
        }
    }
}
