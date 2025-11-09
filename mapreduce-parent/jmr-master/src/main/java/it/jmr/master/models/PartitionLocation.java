package it.jmr.master.models;

import it.jmr.master.WorkerI;

public

class PartitionLocation {
    private final WorkerI worker;
    private final String mapTaskId;
    private final PartitionInfo partition;

    public PartitionLocation(WorkerI worker, String mapTaskId, PartitionInfo partition) {
        this.worker = worker;
        this.mapTaskId = mapTaskId;
        this.partition = partition;
    }

    public WorkerI getWorker() {
        return worker;
    }

    public String getMapTaskId() {
        return mapTaskId;
    }

    public PartitionInfo getPartition() {
        return partition;
    }
}
