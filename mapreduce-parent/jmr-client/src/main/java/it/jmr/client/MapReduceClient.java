package it.jmr.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import it.jmr.grpc.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MapReduceClient {
    private final ManagedChannel channel;
    private final MapReduceServiceGrpc.MapReduceServiceBlockingStub masterBlockingStub;
    private final MapReduceServiceGrpc.MapReduceServiceStub masterAsyncStub;
    private final JarServiceGrpc.JarServiceStub jarAsyncStub;

    public MapReduceClient(String host, int port) {
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(100 * 1024 * 1024) // 100MB per i JAR grandi
                .build();
        this.masterBlockingStub = MapReduceServiceGrpc.newBlockingStub(channel);
        this.masterAsyncStub = MapReduceServiceGrpc.newStub(channel);
        this.jarAsyncStub = JarServiceGrpc.newStub(channel);
    }

 
    /**
     * Carica un JAR sul master
     */
    public String uploadJar(String jarPath) throws IOException, InterruptedException {
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new IOException("JAR file non trovato: " + jarPath);
        }

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final String[] jarId = new String[1];
        final Exception[] exception = new Exception[1];

        StreamObserver<JarChunk> requestObserver = jarAsyncStub.uploadJar(
                new StreamObserver<UploadJarResponse>() {
                    @Override
                    public void onNext(UploadJarResponse response) {
                        if (response.getSuccess()) {
                            jarId[0] = response.getJarId();
                            System.out.println("JAR caricato con ID: " + response.getJarId());
                        } else {
                            exception[0] = new IOException(response.getMessage());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        exception[0] = new IOException("Errore durante il caricamento: " + t.getMessage(), t);
                        finishLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        finishLatch.countDown();
                    }
                });

        try (FileInputStream fis = new FileInputStream(jarFile)) {
            byte[] buffer = new byte[64 * 1024]; // 64KB chunks
            int bytesRead;
            boolean firstChunk = true;
            long totalSize = jarFile.length();
            long uploadedBytes = 0;

            System.out.println("Caricamento JAR: " + jarFile.getName() + " (" + totalSize + " bytes)");

            while ((bytesRead = fis.read(buffer)) != -1) {
                JarChunk.Builder chunkBuilder = JarChunk.newBuilder()
                        .setContent(com.google.protobuf.ByteString.copyFrom(buffer, 0, bytesRead))
                        .setJarName(jarFile.getName());

                if (firstChunk) {
                    chunkBuilder.setTotalSize(totalSize);
                    firstChunk = false;
                }

                requestObserver.onNext(chunkBuilder.build());
                uploadedBytes += bytesRead;

                // Progress indicator
                int percentage = (int) ((uploadedBytes * 100) / totalSize);
                System.out.print("\rProgresso: " + percentage + "% ");
            }
            System.out.println();
        }

        requestObserver.onCompleted();

        if (!finishLatch.await(5, TimeUnit.MINUTES)) {
            throw new IOException("Timeout durante il caricamento del JAR");
        }

        if (exception[0] != null) {
            throw new IOException(exception[0]);
        }

        return jarId[0];
    }

    /**
     * Sottomette un job al master
     */
    public String submitJob(String jarId, String mainClass) {
        SubmitJobRequest.Builder requestBuilder = SubmitJobRequest.newBuilder()
                .setJarId(jarId)
                .setMainClass(mainClass);

        SubmitJobResponse response = masterBlockingStub.submitJob(requestBuilder.build());

        if (!response.getSuccess()) {
            throw new RuntimeException("Job rifiutato: " + response.getMessage());
        }

        System.out.println("Job sottomesso con successo!");
        System.out.println("Job ID: " + response.getJobId());
        return response.getJobId();
    }

    /**
     * Ottiene lo stato di un job
     */
    public void getJobStatus(String jobId) {
        GetJobStatusRequest request = GetJobStatusRequest.newBuilder()
                .setJobId(jobId)
                .build();

        GetJobStatusResponse response = masterBlockingStub.getJobStatus(request);

        if (response.getFound()) {
            JobInfo job = response.getJobInfo();
            System.out.println("\n╔════════════════════════════════════════╗");
            System.out.println("║           Job Status                   ║");
            System.out.println("╚════════════════════════════════════════╝");
            System.out.println("Job ID:       " + job.getJobId());
            System.out.println("Status:       " + job.getStatus());
            System.out.println("Main Class:   " + job.getMainClass());
            System.out.println("Submitted:    " + new java.util.Date(job.getSubmissionTime()));

            if (job.getStartTime() > 0) {
                System.out.println("Started:      " + new java.util.Date(job.getStartTime()));
            }
            if (job.getEndTime() > 0) {
                System.out.println("Ended:        " + new java.util.Date(job.getEndTime()));
                long duration = job.getEndTime() - job.getStartTime();
                System.out.println("Duration:     " + duration + "ms");
            }
            if (!job.getErrorMessage().isEmpty()) {
                System.out.println("Error:        " + job.getErrorMessage());
            }
        } else {
            System.out.println("Job non trovato: " + jobId);
        }
    }

    /**
     * Lista tutti i job
     */
    public void listJobs() {
        ListJobsRequest request = ListJobsRequest.newBuilder().build();
        ListJobsResponse response = masterBlockingStub.listJobs(request);

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║           Lista Job (" + response.getTotalCount() + ")               ║");
        System.out.println("╚════════════════════════════════════════╝\n");

        for (JobInfo job : response.getJobsList()) {
            System.out.printf("%-36s | %-10s | %s\n",
                    job.getJobId().substring(0, 8) + "...",
                    job.getStatus(),
                    job.getMainClass());
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: MapReduceClient <command> <master-host> <master-port> [options]");
            System.out.println();
            System.out.println("Comandi:");
            System.out.println("  submit <jar-path> <main-class> [job-args...]  - Sottomette un job");
            System.out.println("  status <job-id>                               - Ottiene lo stato di un job");
            System.out.println("  list                                          - Lista tutti i job");
            System.out.println("  monitor <job-id>                              - Monitora un job in tempo reale");
            System.out.println();
            System.out.println("Esempio:");
            System.out
                    .println("  java -jar mapreduce-client.jar submit localhost 9999 myjob.jar com.example.MyJob arg1");
            return;
        }

        String command = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);

        MapReduceClient client = new MapReduceClient(host, port);
        try {
            switch (command.toLowerCase()) {
                case "submit":
                    if (args.length < 5) {
                        System.err.println("Uso: submit <jar-path> <main-class>");
                        System.exit(1);
                    }
                    String jarPath = args[3];
                    String mainClass = args[4];

                    String jarId = client.uploadJar(jarPath);
                    String jobId = client.submitJob(jarId, mainClass);
                    System.out.println("\n✓ Job sottomesso: " + jobId);
                    break;

                case "status":
                    if (args.length < 4) {
                        System.err.println("Uso: status <job-id>");
                        System.exit(1);
                    }
                    client.getJobStatus(args[3]);
                    break;

                case "list":
                    client.listJobs();
                    break;

                default:
                    System.err.println("Comando sconosciuto: " + command);
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Errore: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}