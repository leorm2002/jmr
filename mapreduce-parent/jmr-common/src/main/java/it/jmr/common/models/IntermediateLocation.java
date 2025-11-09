package it.jmr.common.models;

public class IntermediateLocation {
    private String workerId;
    private String taskId;
    private String partitionId;

    public IntermediateLocation(String workerId, String taskId, String partitionId) {
        this.workerId = workerId;
        this.taskId = taskId;
        this.partitionId = partitionId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

}
