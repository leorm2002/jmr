package it.jmr.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.jarservice.JarServiceClient;
import it.jmr.common.jarservice.JobServiceClient;
import it.jmr.common.models.JobConfiguration;
import it.jmr.grpc.*;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class MapReduceClient implements AutoCloseable {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MapReduceClient.class);

    private final ManagedChannel channel;
    private final MapReduceServiceGrpc.MapReduceServiceBlockingStub masterBlockingStub;
    private final JarServiceGrpc.JarServiceStub jarAsyncStub;
    private final JobServiceGrpc.JobServiceStub jobAsyncStub;

    public MapReduceClient(String host, int port) {
        LOGGER.info("Creating MapReduceClient for master at {}:{}", host, port);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().maxInboundMessageSize(100 * 1024 * 1024) // 100MB for large JARs
                .build();
        this.masterBlockingStub = MapReduceServiceGrpc.newBlockingStub(channel);
        this.jarAsyncStub = JarServiceGrpc.newStub(channel);
        this.jobAsyncStub = JobServiceGrpc.newStub(channel);
    }

    public String uploadJar(Path jarPath) throws JMRException {
        try {
            LOGGER.info("Uploading jar: {}", jarPath);
            return JarServiceClient.uploadJar(jarPath, jarAsyncStub);
        } catch (JMRException | InterruptedException e) {
            throw new JMRException("Failed to upload jar", e);
        }
    }

    public <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig)
            throws JMRException {
        try {
            LOGGER.info("Uploading job configuration");
            return JobServiceClient.serializeAndUploadJob(jobConfig, jobAsyncStub).getJobId();
        } catch (JMRException | InterruptedException e) {
            throw new JMRException("Failed to upload job", e);
        }
    }

    public String uploadJob(Path jobPath) throws JMRException {
        try {
            LOGGER.info("Uploading serialized job: {}", jobPath);
            return JobServiceClient.uploadJobFromFile(jobPath, jobAsyncStub).getJobId();
        } catch (JMRException | InterruptedException e) {
            throw new JMRException("Failed to upload job", e);
        }
    }

    public String submitJob(String jarId, String jobId) throws JMRException {
        LOGGER.info("Submitting job with jarId: {} and jobId: {}", jarId, jobId);
        final SubmitJobRequest.Builder requestBuilder = SubmitJobRequest.newBuilder().setJarId(jarId).setJobId(jobId);
        final SubmitJobResponse response = masterBlockingStub.submitJob(requestBuilder.build());

        if (!response.getSuccess()) {
            LOGGER.error("Job rejected: {}", response.getMessage());
            throw new JMRException("Job rejected: " + response.getMessage());
        }

        LOGGER.info("Job submitted successfully!");
        LOGGER.info("Job ID: {}", response.getJobId());
        return response.getJobId();
    }

    public String submit(Path jarPath, Path jobPath) throws JMRException {
        final String jarId = uploadJar(jarPath);
        final String jobId = uploadJob(jobPath);
        return submitJob(jarId, jobId);
    }

    public String getJobStatus(String jobId) {
        LOGGER.info("Getting status for job: {}", jobId);
        final GetJobStatusRequest request = GetJobStatusRequest.newBuilder().setJobId(jobId).build();

        final GetJobStatusResponse response = masterBlockingStub.getJobStatus(request);

        if (response.getFound()) {
            JobInfo jobInfo = response.getJobInfo();
            LOGGER.debug("Job status for {}: {}", jobId, jobInfo.getStatus());
            return jobInfo.getStatus().toString();
        } else {
            LOGGER.warn("Job not found: {}", jobId);
            return "Job not found.";
        }
    }

    public void listJobs() {
        LOGGER.info("Listing all jobs");
        ListJobsRequest request = ListJobsRequest.newBuilder().build();
        ListJobsResponse response = masterBlockingStub.listJobs(request);

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║           Job List (" + response.getTotalCount() + ")               ║");
        System.out.println("╚════════════════════════════════════════╝\n");

        for (JobInfo job : response.getJobsList()) {
            System.out.printf("%-36s | %-10s | %s\n", job.getJobId().substring(0, 8) + "...", job.getStatus(), job.getMainClass());
        }
    }

    public void shutdown() throws InterruptedException {
        LOGGER.info("Shutting down MapReduceClient");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }
}
