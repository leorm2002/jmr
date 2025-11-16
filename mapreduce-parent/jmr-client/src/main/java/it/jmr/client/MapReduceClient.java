package it.jmr.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import it.jmr.common.jarservice.JarServiceClient;
import it.jmr.common.jarservice.JobServiceClient;
import it.jmr.common.models.JobConfiguration;
import it.jmr.grpc.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class MapReduceClient {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MapReduceClient.class);

    private final ManagedChannel channel;
    private final MapReduceServiceGrpc.MapReduceServiceBlockingStub masterBlockingStub;
    private final JarServiceGrpc.JarServiceStub jarAsyncStub;
    private final JobServiceGrpc.JobServiceStub jobAsyncStub;

    public MapReduceClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().maxInboundMessageSize(100 * 1024 * 1024) // 100MB per i JAR grandi
                .build();
        this.masterBlockingStub = MapReduceServiceGrpc.newBlockingStub(channel);
        this.jarAsyncStub = JarServiceGrpc.newStub(channel);
        this.jobAsyncStub = JobServiceGrpc.newStub(channel);
    }

    /**
     * Carica un JAR sul master
     */
    public String uploadJar(String jarPath) throws IOException, InterruptedException {
        return JarServiceClient.uploadJar(jarPath, jarAsyncStub);
    }

    /**
     * Carica un JAR sul master
     */
    public <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig)
            throws IOException, InterruptedException {
        return JobServiceClient.uploadJob(jobConfig, jobAsyncStub);
    }

    /**
     * Sottomette un job al master
     */
    public String submitJob(String jarId, String jobId) {
        final SubmitJobRequest.Builder requestBuilder = SubmitJobRequest.newBuilder().setJarId(jarId).setJobId(jobId);
        final SubmitJobResponse response = masterBlockingStub.submitJob(requestBuilder.build());

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
    public String getJobStatus(String jobId) {
        final GetJobStatusRequest request = GetJobStatusRequest.newBuilder().setJobId(jobId).build();

        final GetJobStatusResponse response = masterBlockingStub.getJobStatus(request);

        if (response.getFound()) {
            JobInfo jobInfo = response.getJobInfo();
            return jobInfo.getStatus().toString();
        } else {
            return "Job not found.";
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
            System.out.printf("%-36s | %-10s | %s\n", job.getJobId().substring(0, 8) + "...", job.getStatus(), job.getMainClass());
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
            System.out.println("  java -jar mapreduce-client.jar submit localhost 9999 myjob.jar com.example.MyJob arg1");
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