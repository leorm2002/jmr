package it.jmr.worker.models;

import java.util.List;

import it.jmr.common.PartitionInfo;

public class TaskResult {
    private String jobId;
    private String taskId;
    private List<PartitionInfo> partitions;
    private long executionTime;

    public TaskResult(String jobId, String taskId, List<PartitionInfo> partitions, long executionTime) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.partitions = partitions;
        this.executionTime = executionTime;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    

    // Getters and setters
}
