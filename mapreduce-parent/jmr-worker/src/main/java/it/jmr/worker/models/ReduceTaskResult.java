package it.jmr.worker.models;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.utils.Pair;

public class ReduceTaskResult {
    private String jobId;
    private String taskId;
    private List<Pair<String, Serializable>> reducedData;
    private long executionTime;

    public ReduceTaskResult(String jobId, String taskId, List<Pair<String, Serializable>> reducedData, long executionTime) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.reducedData = reducedData;
        this.executionTime = executionTime;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<Pair<String, Serializable>> getReducedData() {
        return reducedData;
    }

    public long getExecutionTime() {
        return executionTime;
    }
}
