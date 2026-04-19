package it.jmr.worker.storage;

import it.jmr.common.PartitionInfo;

public class InMemoryIntermediateStorage implements IntermediateStorage {
    private final java.util.Map<String, java.util.List<java.io.Serializable>> storage = new java.util.HashMap<>();

    @Override
    public PartitionInfo savePartitionData(String taskId, String partitionId, java.util.List<java.io.Serializable> data) {
        final String full_key = taskId + "_" + partitionId;
        storage.put(full_key, data);
        return new PartitionInfo(taskId, partitionId);
    }

    @Override
    public java.util.List<java.io.Serializable> getPartitionData(String taskId, String partitionId) {
        return storage.get(taskId + "_" + partitionId);
    }

    @Override
    public void deleteTaskData(String taskId) {
        storage.keySet().removeIf(key -> key.startsWith(taskId + "_"));
    }

    @Override
    public void clear() {
        storage.clear();
    }
}
