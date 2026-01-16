package it.jmr.common.models;

public class IntermediateLocation {
    private String workerId;
    private String taskId;
    private String partitionId;
    private String host;
    private int port;

    public IntermediateLocation(String workerId, String taskId, String partitionId, String host, int port) {
        this.workerId = workerId;
        this.taskId = taskId;
        this.partitionId = partitionId;
        this.host = host;
        this.port = port;
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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
