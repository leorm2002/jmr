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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        data = loadDataFilesInParallel(filePath);
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

    private static void logLoadProgress(final Path path, final int currentFile, final int totalFiles, final int recordsInFile,
            final int totalLoadedRecords) {
        final int percentage = totalFiles == 0 ? 100 : (currentFile * 100 / totalFiles);
        System.out.printf("FSDataProvider loading: %d/%d files (%d%%) - %s -> %d records, total=%d%n", currentFile, totalFiles, percentage,
                path.getFileName(), recordsInFile, totalLoadedRecords);
        System.out.flush();
    }

    private static <D extends Serializable> List<D> loadDataFilesInParallel(final List<Path> filePaths) throws JMRException {
        final int totalFiles = filePaths.size();
        final int parallelism = Math.max(1, Math.min(totalFiles, Runtime.getRuntime().availableProcessors()));
        final ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        final AtomicInteger completedFiles = new AtomicInteger(0);
        final AtomicInteger totalLoadedRecords = new AtomicInteger(0);
        final List<Future<List<D>>> futures = new ArrayList<>(totalFiles);

        try {
            for (final Path path : filePaths) {
                futures.add(executor.submit(() -> {
                    final List<D> fileData = readFileData(path);
                    final int currentFile = completedFiles.incrementAndGet();
                    final int loadedRecords = totalLoadedRecords.addAndGet(fileData.size());
                    logLoadProgress(path, currentFile, totalFiles, fileData.size(), loadedRecords);
                    return fileData;
                }));
            }

            final List<D> loadedData = new ArrayList<>();
            for (final Future<List<D>> future : futures) {
                loadedData.addAll(future.get());
            }
            return loadedData;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JMRException("Interrupted while loading data files", e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof JMRException jmrException) {
                throw jmrException;
            }
            throw new JMRException("Failed to load serialized data files", cause);
        } finally {
            executor.shutdownNow();
        }
    }

    private static <D extends Serializable> List<D> readFileData(final Path path) throws JMRException {
        try (final var in = java.nio.file.Files.newInputStream(path); final var ois = new ObjectInputStream(in)) {
            @SuppressWarnings("unchecked")
            final List<D> fileData = ((Container<D>) ois.readObject()).data;
            return fileData;
        } catch (IOException | ClassNotFoundException e) {
            throw new JMRException("Failed to load data from file: " + path, e);
        }
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
