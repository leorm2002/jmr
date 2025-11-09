package it.jmr.common.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceListener;

import it.jmr.common.utils.ExecutorManager;

import java.io.IOException;
import java.net.InetAddress;

import javax.jmdns.ServiceInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DiscoveryService {
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
                jmdns.requestServiceInfo(serviceType, event.getName());
            }

            @Override
            public void serviceRemoved(ServiceEvent event) {
                System.out.println("Servizio rimosso: " + event.getName());
            }

            @Override
            public void serviceResolved(ServiceEvent event) {
                ServiceInfo info = event.getInfo();
                found.add(info);
                System.out.printf("Servizio risolto: %s -> %s:%d%n",
                        info.getName(),
                        info.getInetAddresses()[0].getHostAddress(),
                        info.getPort());
            }
        });

        System.out.printf("Discovery per %d secondi...%n", seconds);

        // esegui attesa su virtual thread
        CompletableFuture<Void> waitFuture = CompletableFuture.runAsync(() -> {
            try {Thread.sleep(seconds * 1000L); } catch (InterruptedException ignored) {}
        }, ExecutorManager.getExecutor());

        waitFuture.join(); // bloccante per il chiamante
        
        jmdns.close();
        System.out.printf("Discovery terminato. Servizi trovati: %d%n", found.size());
        return new ArrayList<>(found);
    }
}
