package it.jmr.grpcdataprovider.remotegrpc;

import java.io.IOException;
import java.io.Serializable;

import it.jmr.common.providers.DataProvider;
import it.jmr.common.providers.DataProviderClient;

/**
 * Implementazione di DataProvider che si connette a un server gRPC esterno. Non
 * gestisce dati localmente, ma delega tutto a un servizio remoto esistente.
 * Utile per accedere a dati ospitati su server esterni.
 */
public class RemoteGrpcDataProvider<D extends Serializable> implements DataProvider<D> {
    static final long serialVersionUID = 1L;

    private final String remoteHost;
    private final int remotePort;
    private volatile boolean initialized = false;

    /**
     * Crea un provider che si connette a un server gRPC esterno.
     *
     * @param remoteHost indirizzo IP o hostname del server esterno
     * @param remotePort porta del server esterno
     */
    public RemoteGrpcDataProvider(String remoteHost, int remotePort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        init();
    }

    @Override
    public DataProvider<D> init() {
        // Non c'è nessun worker locale da avviare
        // Verifica solo che i parametri siano validi
        if (remoteHost == null || remoteHost.isEmpty()) {
            throw new IllegalStateException("Remote host cannot be null or empty");
        }
        if (remotePort <= 0 || remotePort > 65535) {
            throw new IllegalStateException("Invalid remote port: " + remotePort);
        }

        initialized = true;
        System.out.println("RemoteGrpcDataProvider configured to connect to " + remoteHost + ":" + remotePort);
        return this;
    }

    @Override
    public DataProviderClient<D> getClient() {
        if (!initialized) {
            throw new IllegalStateException("Provider not initialized. Call init() first.");
        }

        // Crea un client che punta direttamente al server esterno
        return new RemoteGrpcDataProviderClient<>(remoteHost, remotePort);
    }

    @Override
    public void close() throws IOException {
        // Non c'è nessun server locale da chiudere
        initialized = false;
        System.out.println("RemoteGrpcDataProvider closed");
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public int getRemotePort() {
        return remotePort;
    }
}