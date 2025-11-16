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
import it.jmr.common.utils.JMRLog;
import it.jmr.grpc.JarChunk;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.UploadJarResponse;

public class JarServiceClient {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JarServiceClient.class);

    /**
     * Carica un JAR sul master
     */
    public static String uploadJar(String jarPath, JarServiceGrpc.JarServiceStub jarAsyncStub) throws IOException, InterruptedException {

        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IOException("JAR file non trovato: " + jarPath);
        }

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicReference<String> jarId = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();

        StreamObserver<JarChunk> requestObserver = jarAsyncStub.uploadJar(new StreamObserver<UploadJarResponse>() {
            @Override
            public void onNext(UploadJarResponse response) {
                if (response.getSuccess()) {
                    jarId.set(response.getJarId());
                    JMRLog.info(LOGGER, "JAR caricato con successo - ID: {}", response.getJarId());
                } else {
                    exception.set(new IOException(response.getMessage()));
                    JMRLog.error(LOGGER, "Caricamento JAR fallito: {}", response.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                exception.set(new IOException("Errore durante il caricamento del JAR", t));
                JMRLog.error(LOGGER, "Errore durante il caricamento del JAR: {}", t.getMessage(), t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                JMRLog.debug(LOGGER, "Upload completato");
                finishLatch.countDown();
            }
        });

        try (FileInputStream fis = new FileInputStream(jarFile)) {
            byte[] buffer = new byte[JMRConstants.UPLOAD_CHUNK_SIZE];
            int bytesRead;
            boolean firstChunk = true;
            long totalSize = jarFile.length();
            long uploadedBytes = 0;

            JMRLog.info(LOGGER, "Inizio caricamento JAR: {} ({} bytes)", jarFile.getName(), totalSize);

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
                    JMRLog.debug(LOGGER, "Progresso upload: {}%", percentage);
                }
            }

            JMRLog.info(LOGGER, "Invio completato, in attesa di conferma...");
        }

        requestObserver.onCompleted();

        if (!finishLatch.await(5, TimeUnit.MINUTES)) {
            JMRLog.error(LOGGER, "Timeout durante il caricamento del JAR dopo 5 minuti");
            throw new IOException("Timeout durante il caricamento del JAR");
        }

        if (exception.get() != null) {
            throw new IOException(exception.get());
        }

        if (jarId.get() == null) {
            JMRLog.error(LOGGER, "Caricamento completato ma nessun JAR ID ricevuto");
            throw new IOException("Nessun JAR ID ricevuto dal server");
        }

        return jarId.get();
    }
}
