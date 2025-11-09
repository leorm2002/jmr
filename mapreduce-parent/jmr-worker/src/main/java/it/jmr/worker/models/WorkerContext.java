package it.jmr.worker.models;

import java.util.concurrent.ConcurrentHashMap;

import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.utils.Pair;
import it.jmr.worker.storage.IntermediateStorage;

/**
 * Represents the shared data and state of a worker node in the MapReduce
 * framework.
 */
public class WorkerContext {
    public boolean busy = false;
    public final IntermediateStorage intermediateStorage;
    public final String workerId;
    public final ConcurrentHashMap<String, String> jarStorage;
    public final ConcurrentHashMap<Pair<String, String>, WorkerTaskStatus> statusMap;
    public final ConcurrentHashMap<Pair<String, String>, TaskResult> taskResults = new ConcurrentHashMap<>();

    public WorkerContext(IntermediateStorage inMemoryIntermediateStorage, String workerId) {
        this.intermediateStorage = inMemoryIntermediateStorage;
        this.workerId = workerId;
        this.jarStorage = new ConcurrentHashMap<>();
        this.statusMap = new ConcurrentHashMap<>();
    }

}
