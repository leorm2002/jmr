package it.jmr.master.models;

public class PartitionInfo {
    int partitionId;
    int numRecords;

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecord) {
        this.numRecords = numRecord;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

}
