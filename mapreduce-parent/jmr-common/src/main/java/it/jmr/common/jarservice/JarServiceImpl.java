package it.jmr.common.jarservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JmrUtils;
import it.jmr.grpc.JarChunk;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.UploadJarResponse;
// Removed import it.jmr.master.MasterContext; // Added import

public class JarServiceImpl extends JarServiceGrpc.JarServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JarServiceImpl.class);

    private final ConcurrentHashMap<String, Path> jarStorage;
    private final Path jarStorageDir;
    private final ResourceUploadedCallback callback; // Added field

    public JarServiceImpl(Path jarStorageDir, ConcurrentHashMap<String, Path> jarStorage, ResourceUploadedCallback callback) {
        this.jarStorageDir = jarStorageDir;
        this.jarStorage = jarStorage;
        this.callback = callback;
    }

    @Override
    public StreamObserver<JarChunk> uploadJar(StreamObserver<UploadJarResponse> responseObserver) {
        return new StreamObserver<JarChunk>() {
            private FileOutputStream fos;
            private String jarId;
            private Path jarPath;
            private long totalSize;
            private long receivedBytes = 0;

            @Override
            public void onNext(JarChunk chunk) {
                try {
                    if (fos == null) {
                        // First chunk - initialize
                        // Use provided jar_id if present, otherwise generate a new one
                        String providedId = chunk.getJarId();
                        jarId = JmrUtils.isEmpty(providedId) ? JmrUtils.generateJarId() : providedId;
                        jarPath = jarStorageDir.resolve(jarId + ".jar");
                        totalSize = chunk.getTotalSize();
                        fos = new FileOutputStream(jarPath.toFile());
                        LOGGER.info("Receiving JAR: {} ({} bytes) with ID: {}", chunk.getJarName(), totalSize, jarId);
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
                    callback.onJarUploaded(jarId, jarPath); // Invoke callback

                    UploadJarResponse response = UploadJarResponse.newBuilder().setSuccess(true).setJarId(jarId)
                            .setMessage("JAR uploaded successfully").build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                } catch (Exception e) {
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
                        final File file = jarPath.toFile();
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
