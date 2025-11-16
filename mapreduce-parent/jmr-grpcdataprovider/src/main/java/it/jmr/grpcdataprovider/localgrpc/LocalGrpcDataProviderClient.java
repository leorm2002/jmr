package it.jmr.grpcdataprovider.localgrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import it.jmr.common.JMRConstants;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.JMRLog;
import it.jmr.grpcdataprovider.grpc.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client remoto serializzabile per connettersi a un FSDataProvider worker. Può
 * essere inviato dal master ai worker remoti per accesso distribuito ai dati.
 */
public class LocalGrpcDataProviderClient<D extends Serializable> implements DataProviderClient<D> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalGrpcDataProviderClient.class);
    private static final long serialVersionUID = 1L;

    private String host;
    private int port;

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    // Questi campi non sono serializzabili, vengono ricreati dopo la
    // deserializzazione
    private transient ManagedChannel channel;
    private transient DataProviderServiceGrpc.DataProviderServiceBlockingStub blockingStub;

    public LocalGrpcDataProviderClient() {
    }

    public LocalGrpcDataProviderClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void init() {
        if (channel != null) {
            return; // Già inizializzato
        }

        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().maxInboundMessageSize(JMRConstants.MAX_INBOUND_MESSAGE_SIZE).build();
        blockingStub = DataProviderServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public long size() throws JMRException {
        ensureInitialized();
        try {
            SizeRequest request = SizeRequest.newBuilder().build();
            SizeResponse response = blockingStub.getSize(request);
            return response.getSize();
        } catch (Exception e) {
            throw new JMRException("Failed to get size from remote worker at " + host + ":" + port, e);
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
            JMRLog.error(LOGGER, "Failed to fetch chunk from remote worker at {}:{} ({})", host, port, e.getMessage());
            throw new JMRException("Failed to fetch chunk from remote worker" + e.getMessage(), e);
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
        return "FSDataProviderClient{host='" + host + "', port=" + port + "}";
    }
}