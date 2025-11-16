package it.jmr.common.jarservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import it.jmr.common.JMRConstants;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JMRLog;
import it.jmr.grpc.JarChunk;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.UploadJarResponse;

public class JarServiceClient {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JarServiceClient.class);

    /**
     * Uploads a JAR to the master
     */
    public static String uploadJar(String jarPath, JarServiceGrpc.JarServiceStub jarAsyncStub) throws JMRException, InterruptedException {

        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new JMRException("JAR file not found: " + jarPath);
        }

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicReference<String> jarId = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();

        StreamObserver<JarChunk> requestObserver = jarAsyncStub.uploadJar(new StreamObserver<UploadJarResponse>() {
            @Override
            public void onNext(UploadJarResponse response) {
                if (response.getSuccess()) {
                    jarId.set(response.getJarId());
                    JMRLog.info(LOGGER, "JAR uploaded successfully - ID: {}", response.getJarId());
                } else {
                    exception.set(new JMRException(response.getMessage()));
                    JMRLog.error(LOGGER, "JAR upload failed: {}", response.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                exception.set(new JMRException("Error during JAR upload", t));
                JMRLog.error(LOGGER, "Error during JAR upload: {}", t.getMessage(), t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                JMRLog.debug(LOGGER, "Upload completed");
                finishLatch.countDown();
            }
        });

        try (FileInputStream fis = new FileInputStream(jarFile)) {
            byte[] buffer = new byte[JMRConstants.UPLOAD_CHUNK_SIZE];
            int bytesRead;
            boolean firstChunk = true;
            long totalSize = jarFile.length();
            long uploadedBytes = 0;

            JMRLog.info(LOGGER, "Starting JAR upload: {} ({} bytes)", jarFile.getName(), totalSize);

            while ((bytesRead = fis.read(buffer)) != -1) {
                JarChunk.Builder chunkBuilder = JarChunk.newBuilder().setContent(ByteString.copyFrom(buffer, 0, bytesRead))
                        .setJarName(jarFile.getName());

                if (firstChunk) {
                    chunkBuilder.setTotalSize(totalSize);
                    firstChunk = false;
                }

                requestObserver.onNext(chunkBuilder.build());
                uploadedBytes += bytesRead;

                int percentage = (int) ((uploadedBytes * 100) / totalSize);
                if (percentage % 10 == 0 || uploadedBytes == totalSize) {
                    JMRLog.debug(LOGGER, "Upload progress: {}%", percentage);
                }
            }

            JMRLog.info(LOGGER, "Upload complete, waiting for confirmation...");
        } catch (IOException e) {
            throw new JMRException("Error reading JAR file", e);
        }

        requestObserver.onCompleted();

        if (!finishLatch.await(JMRConstants.JAR_UPLOAD_TIMEOUT_M, TimeUnit.MINUTES)) {
            JMRLog.error(LOGGER, "Timeout during JAR upload after {} minutes", JMRConstants.JAR_UPLOAD_TIMEOUT_M);
            throw new JMRException("Timeout during JAR upload");
        }

        if (exception.get() != null) {
            throw new JMRException(exception.get().getMessage(), exception.get());
        }

        if (jarId.get() == null) {
            JMRLog.error(LOGGER, "Upload completed but no JAR ID received");
            throw new JMRException("No JAR ID received from server");
        }

        return jarId.get();
    }
}
