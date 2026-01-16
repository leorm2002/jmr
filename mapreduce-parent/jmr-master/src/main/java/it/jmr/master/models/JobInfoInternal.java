package it.jmr.master.models;

import it.jmr.grpc.JobInfo;
import it.jmr.grpc.JobStatus;

public class JobInfoInternal {
    private final String jobId;
    private final long submissionTime;

    private String jobPath;
    private String jarPath;
    private JobStatus status;
    private String errorMessage = "";
    private long startTime;
    private long endTime;
    private String jarId;

    public String getJobPath() {
        return jobPath;
    }

    public static JobInfoInternal recievedJob(String jobId) {
        return new JobInfoInternal(jobId);
    }

    public void recievedJarFound(String jarPth, String jarId) {
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

    public void recievedSerializedJobFound(String jobPth) {
        this.jobPath = jobPth;
    }

    private JobInfoInternal(String jobId) {
        this.jobId = jobId;
        this.submissionTime = System.currentTimeMillis();
        this.status = JobStatus.PENDING;
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

    public JobInfo toProto() {
        JobInfo.Builder builder = JobInfo.newBuilder().setJobId(jobId).setStatus(status).setSubmissionTime(submissionTime).setStartTime(startTime)
                .setEndTime(endTime).setErrorMessage(errorMessage);

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

    public String getJarPath() {
        return jarPath;
    }

    public JobStatus getStatus() {
        return status;
    }

    public String getJarId() {
        return jarId;
    }

    @Override
    public String toString() {
        return "JobInfoInternal [jobId=" + jobId + ", jarPath=" + jarPath + "]";
    }
}
