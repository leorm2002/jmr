package it.jmr.grpcdataprovider.remotegrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.grpcdataprovider.grpc.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client per accedere a dati da un server gRPC esterno. Serializzabile, può
 * essere inviato ai worker remoti.
 */
public class RemoteGrpcDataProviderClient<D extends Serializable> implements DataProviderClient<D> {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;

    // Campi transient ricreati dopo deserializzazione
    private transient ManagedChannel channel;
    private transient DataProviderServiceGrpc.DataProviderServiceBlockingStub blockingStub;

    /**
     * Crea un client che si connette a un server gRPC esterno.
     *
     * @param host indirizzo IP o hostname del server
     * @param port porta del server
     */
    public RemoteGrpcDataProviderClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void init() {
        if (channel != null) {
            return; // Già inizializzato
        }

        blockingStub = DataProviderServiceGrpc.newBlockingStub(channel);
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

        System.out.println("RemoteGrpcDataProviderClient connected to " + host + ":" + port);
    }

    @Override
    public long size() throws JMRException {
        ensureInitialized();
        try {
            SizeRequest request = SizeRequest.newBuilder().build();
            SizeResponse response = blockingStub.getSize(request);
            return response.getSize();
        } catch (Exception e) {
            throw new JMRException("Failed to get size from remote server at " + host + ":" + port, e);
        }
    }

    @Override
    public List<D> fetchChunk(long offset, long limit) throws JMRException {
        ensureInitialized();
        try {
            ChunkRequest request = ChunkRequest.newBuilder().setOffset(offset).setLimit(limit).build();

            ChunkResponse response = blockingStub.fetchChunk(request);

            List<D> result = new ArrayList<>();
            for (com.google.protobuf.ByteString bytes : response.getDataList()) {
                D item = deserializeObject(bytes.toByteArray());
                result.add(item);
            }

            return result;
        } catch (Exception e) {
            throw new JMRException("Failed to fetch chunk from remote server at " + host + ":" + port, e);
        }
    }

    @Override
    public void close() throws JMRException {
        if (channel != null) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
                System.out.println("RemoteGrpcDataProviderClient disconnected from " + host + ":" + port);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void ensureInitialized() throws JMRException {
        if (channel == null || blockingStub == null) {
            throw new JMRException("Client not initialized. Call init() first.");
        }
    }

    @SuppressWarnings("unchecked")
    private D deserializeObject(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (D) ois.readObject();
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "RemoteGrpcDataProviderClient{host='" + host + "', port=" + port + "}";
    }
}