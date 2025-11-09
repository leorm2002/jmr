package it.jmr.master.models;
import it.jmr.grpc.JobInfo;
import it.jmr.grpc.JobStatus;

public class JobInfoInternal {
    private final String jobId;
    private final String mainClass;
    private final String jarPath;
    private JobStatus status;
    private String errorMessage = "";
    private final long submissionTime;
    private long startTime;
    private long endTime;

    public JobInfoInternal(String jobId, String mainClass, String jarPath) {
        this.jobId = jobId;
        this.mainClass = mainClass;
        this.jarPath = jarPath;
        this.status = JobStatus.PENDING;
        this.submissionTime = System.currentTimeMillis();
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
        JobInfo.Builder builder = JobInfo.newBuilder()
                .setJobId(jobId)
                .setMainClass(mainClass)
                .setStatus(status)
                .setSubmissionTime(submissionTime)
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setErrorMessage(errorMessage);

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

    public String getMainClass() {
        return mainClass;
    }

    public String getJarPath() {
        return jarPath;
    }

    public JobStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "JobInfoInternal [jobId=" + jobId + ", mainClass=" + mainClass + ", jarPath=" + jarPath + "]";
    }
}
