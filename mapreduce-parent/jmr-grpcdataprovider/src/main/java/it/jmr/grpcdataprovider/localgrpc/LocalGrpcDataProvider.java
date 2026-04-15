package it.jmr.grpcdataprovider.localgrpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import it.jmr.common.JMRConstants;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.providers.DataProvider;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.grpcdataprovider.Container;
import it.jmr.grpcdataprovider.grpc.*;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementazione di DataProvider che carica dati da file e li serve tramite
 * gRPC. Può creare client remoti serializzabili per l'accesso distribuito.
 */
public class LocalGrpcDataProvider<D extends Serializable> implements DataProvider<D> {

    private final List<D> data;
    private Server grpcServer;
    private volatile boolean initialized = false;
    private final Object lock = new Object();
    private int serverPort;
    // Can be overridden for distributed deployment
    private String serverHost = "localhost";

    public void setServerHost(final String serverHost) {
        this.serverHost = serverHost;
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public LocalGrpcDataProvider(Path filePath) throws JMRException {
        try (var in = java.nio.file.Files.newInputStream(filePath); var ois = new ObjectInputStream(in)) {
            data = ((Container<D>) ois.readObject()).data;
            init();
        } catch (IOException | ClassNotFoundException e) {
            throw new JMRException("Failed to load data from file: " + filePath, e);
        }
    }

    public LocalGrpcDataProvider(List<Path> filePath) throws JMRException {
        List<D> loadedData = new ArrayList<>();
        for (Path path : filePath) {
            try (var in = java.nio.file.Files.newInputStream(path); var ois = new ObjectInputStream(in)) {
                loadedData.addAll(((Container<D>) ois.readObject()).data);
            } catch (IOException | ClassNotFoundException e) {
                throw new JMRException("Failed to load data from file: " + path, e);
            }
        }
        data = loadedData;
        init();
    }

    /**
     * Costruttore con host configurabile per deployment distribuito.
     */
    public LocalGrpcDataProvider(Path filePath, String host) throws JMRException {
        this(filePath);
        this.serverHost = host;
    }

    @Override
    public DataProvider<D> init() {
        synchronized (lock) {
            if (initialized) {
                return this;
            }

            try {
                // Crea il server gRPC su una porta dinamica
                grpcServer = ServerBuilder.forPort(0) // 0 = porta dinamica
                        .addService(new DataProviderServiceImpl()).maxInboundMessageSize(JMRConstants.MAX_INBOUND_MESSAGE_SIZE).build().start();

                serverPort = grpcServer.getPort();
                initialized = true;

                System.out.println("FSDataProvider worker started on " + serverHost + ":" + serverPort);

            } catch (IOException e) {
                throw new RuntimeException("Failed to start gRPC server", e);
            }
        }
        return this;
    }

    @Override
    public DataProviderClient<D> getClient() {
        ensureInitialized();
        return new LocalGrpcDataProviderClient<>(serverHost, serverPort);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (grpcServer != null) {
                try {
                    grpcServer.shutdown();
                    if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                        grpcServer.shutdownNow();
                    }
                    System.out.println("FSDataProvider worker stopped");
                } catch (InterruptedException e) {
                    grpcServer.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            initialized = false;
        }
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("DataProvider not initialized. Call init() first.");
        }
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getServerHost() {
        return serverHost;
    }

    // Implementazione del servizio gRPC
    private class DataProviderServiceImpl extends DataProviderServiceGrpc.DataProviderServiceImplBase {

        @Override
        public void getSize(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
            try {
                SizeResponse response = SizeResponse.newBuilder().setSize(data.size()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void fetchChunk(ChunkRequest request, StreamObserver<ChunkResponse> responseObserver) {
            try {
                long offset = request.getOffset();
                long limit = request.getLimit();

                List<D> chunk = new ArrayList<>();
                if (offset >= 0 && offset < data.size()) {
                    long endIndex = Math.min(offset + limit, data.size());
                    chunk = new ArrayList<>(data.subList((int) offset, (int) endIndex));
                }

                // Serializza ogni oggetto
                ChunkResponse.Builder responseBuilder = ChunkResponse.newBuilder();
                for (D item : chunk) {
                    byte[] serialized = serializeObject(item);
                    responseBuilder.addData(com.google.protobuf.ByteString.copyFrom(serialized));
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        private byte[] serializeObject(D obj) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(obj);
            }
            return baos.toByteArray();
        }
    }
}
