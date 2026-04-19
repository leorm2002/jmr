package it.jmr.worker.models;

public class ReduceTaskResult {
    private final String jobId;
    private final String taskId;
    private final int recordCount;
    private final long executionTime;

    public ReduceTaskResult(String jobId, String taskId, int recordCount, long executionTime) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.recordCount = recordCount;
        this.executionTime = executionTime;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public long getExecutionTime() {
        return executionTime;
    }
}
