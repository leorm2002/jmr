package it.jmr.worker.storage;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.utils.Pair;

public interface ReduceResultStorage {
    void saveReducedData(String jobId, String taskId, List<Pair<String, Serializable>> reducedData);

    List<Pair<String, Serializable>> getReducedData(String jobId, String taskId);

    void deleteTaskResult(String jobId, String taskId);

    void clear();
}
