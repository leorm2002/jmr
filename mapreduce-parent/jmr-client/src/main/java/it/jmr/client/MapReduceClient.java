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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class MapReduceClient implements AutoCloseable {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MapReduceClient.class);
    private static final DateTimeFormatter JOB_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final ManagedChannel channel;
    private final MapReduceServiceGrpc.MapReduceServiceBlockingStub masterBlockingStub;
    private final JarServiceGrpc.JarServiceStub jarAsyncStub;
    private final JobServiceGrpc.JobServiceStub jobAsyncStub;

    public record JobProgressSnapshot(boolean found, String status, int mapProgress, int reduceProgress, String errorMessage) {
        public boolean isTerminal() {
            return "COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status) || !found;
        }
    }

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
        return getJobProgress(jobId).status();
    }

    public JobProgressSnapshot getJobProgress(String jobId) {
        LOGGER.debug("Getting status for job: {}", jobId);
        final GetJobStatusRequest request = GetJobStatusRequest.newBuilder().setJobId(jobId).build();

        final GetJobStatusResponse response = masterBlockingStub.getJobStatus(request);

        if (response.getFound()) {
            final JobInfo jobInfo = response.getJobInfo();
            LOGGER.debug("Job status for {}: {}", jobId, jobInfo.getStatus());
            return new JobProgressSnapshot(true, jobInfo.getStatus().toString(), jobInfo.getMapProgress(), jobInfo.getReduceProgress(),
                    jobInfo.getErrorMessage());
        } else {
            LOGGER.warn("Job not found: {}", jobId);
            return new JobProgressSnapshot(false, "Job not found.", 0, 0, "Job not found.");
        }
    }

    public void cancelJob(String jobId) throws JMRException {
        LOGGER.info("Cancelling job: {}", jobId);
        final CancelJobRequest request = CancelJobRequest.newBuilder().setJobId(jobId).build();
        final CancelJobResponse response = masterBlockingStub.cancelJob(request);

        if (!response.getSuccess()) {
            throw new JMRException("Failed to cancel job: " + response.getMessage());
        }
    }

    public byte[] getJobResult(String jobId) throws JMRException {
        LOGGER.info("Fetching serialized result for job: {}", jobId);
        final GetJobResultRequest request = GetJobResultRequest.newBuilder().setJobId(jobId).build();
        final GetJobResultResponse response = masterBlockingStub.getJobResult(request);

        if (!response.getFound()) {
            throw new JMRException("Job not found: " + jobId);
        }
        if (!response.getAvailable()) {
            throw new JMRException(response.getMessage());
        }

        return response.getSerializedResult().toByteArray();
    }

    public void listJobs() {
        LOGGER.info("Listing all jobs");
        ListJobsRequest request = ListJobsRequest.newBuilder().build();
        ListJobsResponse response = masterBlockingStub.listJobs(request);

        System.out.println();
        System.out.printf("%-16s %-12s %-19s %s%n", "Job ID", "Status", "Submitted", "Error");
        for (JobInfo job : response.getJobsList().stream().sorted(Comparator.comparingLong(JobInfo::getSubmissionTime)).toList()) {
            final String shortJobId = job.getJobId().length() > 12 ? job.getJobId().substring(0, 12) + "..." : job.getJobId();
            final String submittedAt = JOB_TIME_FORMATTER.format(Instant.ofEpochMilli(job.getSubmissionTime()));
            System.out.printf("%-16s %-12s %-19s %s%n", shortJobId, job.getStatus(), submittedAt, job.getErrorMessage());
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
