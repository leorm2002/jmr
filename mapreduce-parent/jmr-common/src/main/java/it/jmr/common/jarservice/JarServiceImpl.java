package it.jmr.common.jarservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.exceptions.JMRException;
import it.jmr.grpc.JarChunk;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.UploadJarResponse;

public class JarServiceImpl extends JarServiceGrpc.JarServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JarServiceImpl.class);

    private final ConcurrentHashMap<String, String> jarStorage;
    private final String jarStorageDir;

    public JarServiceImpl(String jarStorageDir, ConcurrentHashMap<String, String> jarStorage) {
        this.jarStorageDir = jarStorageDir;
        this.jarStorage = jarStorage;
    }

    @Override
    public StreamObserver<JarChunk> uploadJar(StreamObserver<UploadJarResponse> responseObserver) {
        return new StreamObserver<JarChunk>() {
            private FileOutputStream fos;
            private String jarId;
            private String jarPath;
            private long totalSize;
            private long receivedBytes = 0;

            @Override
            public void onNext(JarChunk chunk) {
                try {
                    if (fos == null) {
                        // First chunk - initialize
                        jarId = UUID.randomUUID().toString();
                        jarPath = jarStorageDir + "/" + jarId + ".jar";
                        totalSize = chunk.getTotalSize();
                        fos = new FileOutputStream(jarPath);
                        LOGGER.info("Receiving JAR: {} ({} bytes)", chunk.getJarName(), totalSize);
                    }

                    chunk.getContent().writeTo(fos);
                    receivedBytes += chunk.getContent().size();
                    LOGGER.trace("Received {} bytes for JAR {}", receivedBytes, jarId);

                } catch (IOException e) {
                    LOGGER.error("Error receiving JAR chunk", e);
                    responseObserver.onError(new JMRException("Error receiving JAR chunk", e));
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.error("Error during JAR upload", t);
                cleanup();
            }

            @Override
            public void onCompleted() {
                try {
                    if (fos != null) {
                        fos.close();
                    }

                    jarStorage.put(jarId, jarPath);
                    LOGGER.info("JAR uploaded: {} (ID: {})", jarPath, jarId);

                    UploadJarResponse response = UploadJarResponse.newBuilder().setSuccess(true).setJarId(jarId)
                            .setMessage("JAR uploaded successfully").build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                } catch (IOException e) {
                    LOGGER.error("Error completing JAR upload", e);
                    responseObserver.onError(new JMRException("Error completing JAR upload", e));
                }
            }

            private void cleanup() {
                try {
                    if (fos != null) {
                        fos.close();
                    }
                    if (jarPath != null) {
                        final File file = new File(jarPath);
                        if (file.exists()) {
                            if (file.delete()) {
                                LOGGER.info("Cleaned up partially uploaded JAR: {}", jarPath);
                            } else {
                                LOGGER.warn("Failed to delete partially uploaded JAR: {}", jarPath);
                            }
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error("Error during cleanup", e);
                }
            }
        };
    }

}