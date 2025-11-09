package it.jmr.common;

public class PartitionInfo {
    String partitionId;
    String key;

    public PartitionInfo(String partitionId, String key) {
        this.partitionId = partitionId;
        this.key = key;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getKey() {
        return key;
    }
}
