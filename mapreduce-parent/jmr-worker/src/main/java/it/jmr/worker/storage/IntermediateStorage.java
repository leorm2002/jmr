package it.jmr.worker.storage;

import java.io.Serializable;

import it.jmr.common.PartitionInfo;

public interface IntermediateStorage {

    PartitionInfo savePartitionData(String taskId, String partitionId, java.util.List<Serializable> data);

    java.util.List<Serializable> getPartitionData(String taskId, String partitionId);

    void deleteTaskData(String taskId);

    void clear();
}
