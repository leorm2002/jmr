package it.jmr.master.models;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import it.jmr.grpc.JobInfo;
import it.jmr.grpc.JobStatus;

public class JobInfoInternal {
    private final String jobId;
    private final long submissionTime;
    private final AtomicBoolean cancellationRequested;

    private Path jobPath;
    private Path jarPath;
    private volatile JobStatus status;
    private volatile String errorMessage = "";
    private long startTime;
    private long endTime;
    private String jarId;
    private volatile byte[] serializedResult;
    private volatile int mapProgress;
    private volatile int reduceProgress;

    public Path getJobPath() {
        return jobPath;
    }

    public static JobInfoInternal recievedJob(String jobId) {
        return new JobInfoInternal(jobId);
    }

    public void recievedJarFound(Path jarPth, String jarId) {
        this.jarPath = jarPth;
        this.jarId = jarId;
    }

    public void recievedJarNotFound() {
        this.errorMessage = "JAR not found.";
        this.status = JobStatus.FAILED;
    }

    public void recievedSerializedJobNotFound() {
        this.errorMessage = "Serialized job file not found.";
        this.status = JobStatus.FAILED;
    }

    public void recievedSerializedJobFound(Path jobPth) {
        this.jobPath = jobPth;
    }

    private JobInfoInternal(String jobId) {
        this.jobId = jobId;
        this.submissionTime = System.currentTimeMillis();
        this.status = JobStatus.PENDING;
        this.cancellationRequested = new AtomicBoolean(false);
        this.mapProgress = 0;
        this.reduceProgress = 0;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
        if (status == JobStatus.RUNNING) {
            this.startTime = System.currentTimeMillis();
        } else if (status == JobStatus.COMPLETED || status == JobStatus.FAILED || status == JobStatus.CANCELLED) {
            this.endTime = System.currentTimeMillis();
        }
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setMapProgress(int mapProgress) {
        this.mapProgress = clampProgress(mapProgress);
    }

    public void setReduceProgress(int reduceProgress) {
        this.reduceProgress = clampProgress(reduceProgress);
    }

    public int getMapProgress() {
        return mapProgress;
    }

    public int getReduceProgress() {
        return reduceProgress;
    }

    public boolean requestCancellation() {
        cancellationRequested.set(true);
        setStatus(JobStatus.CANCELLED);
        if (errorMessage.isBlank()) {
            errorMessage = "Job cancelled by user.";
        }
        return true;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    public boolean isTerminal() {
        return status == JobStatus.COMPLETED || status == JobStatus.FAILED || status == JobStatus.CANCELLED;
    }

    public void setSerializedResult(byte[] serializedResult) {
        this.serializedResult = serializedResult == null ? null : Arrays.copyOf(serializedResult, serializedResult.length);
    }

    public byte[] getSerializedResult() {
        return serializedResult == null ? null : Arrays.copyOf(serializedResult, serializedResult.length);
    }

    public boolean hasSerializedResult() {
        return serializedResult != null && serializedResult.length > 0;
    }

    public void clearSerializedResult() {
        this.serializedResult = null;
    }

    public JobInfo toProto() {
        JobInfo.Builder builder = JobInfo.newBuilder().setJobId(jobId).setStatus(status).setSubmissionTime(submissionTime).setStartTime(startTime)
                .setEndTime(endTime).setErrorMessage(errorMessage).setMapProgress(mapProgress).setReduceProgress(reduceProgress);

        return builder.build();
    }

    public long getExecutionTime() {
        if (startTime > 0 && endTime > 0) {
            return endTime - startTime;
        }
        return 0;
    }

    // Getters
    public String getJobId() {
        return jobId;
    }

    public long getSubmissionTime() {
        return submissionTime;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public JobStatus getStatus() {
        return status;
    }

    public String getJarId() {
        return jarId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    private static int clampProgress(int progress) {
        return Math.max(0, Math.min(100, progress));
    }

    @Override
    public String toString() {
        return "JobInfoInternal [jobId=" + jobId + ", jarPath=" + jarPath + "]";
    }
}
