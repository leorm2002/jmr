package it.jmr.common.jarservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.stub.StreamObserver;
import it.jmr.grpc.JarChunk;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.UploadJarResponse;

public class JarServiceImpl extends JarServiceGrpc.JarServiceImplBase {

    private ConcurrentHashMap<String, String> jarStorage;

    private String jarStorageDir;

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
                        // Primo chunk - inizializza
                        jarId = UUID.randomUUID().toString();
                        jarPath = jarStorageDir + "/" + jarId + ".jar";
                        totalSize = chunk.getTotalSize();
                        fos = new FileOutputStream(jarPath);
                        System.out.println("Ricezione JAR: " + chunk.getJarName() + " (" + totalSize + " bytes)");
                    }

                    chunk.getContent().writeTo(fos);
                    receivedBytes += chunk.getContent().size();

                } catch (IOException e) {
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Errore durante il caricamento del JAR: " + t.getMessage());
                cleanup();
            }

            @Override
            public void onCompleted() {
                try {
                    if (fos != null) {
                        fos.close();
                    }

                    jarStorage.put(jarId, jarPath);
                    System.out.println("JAR caricato: " + jarPath + " (ID: " + jarId + ")");

                    UploadJarResponse response = UploadJarResponse.newBuilder()
                            .setSuccess(true)
                            .setJarId(jarId)
                            .setMessage("JAR caricato con successo")
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                } catch (IOException e) {
                    responseObserver.onError(e);
                }
            }

            private void cleanup() {
                try {
                    if (fos != null) {
                        fos.close();
                    }
                    if (jarPath != null) {
                        new File(jarPath).delete();
                    }
                } catch (IOException ignored) {
                }
            }
        };
    }

}