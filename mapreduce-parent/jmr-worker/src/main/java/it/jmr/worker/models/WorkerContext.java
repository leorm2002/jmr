package it.jmr.worker.models;

import java.nio.file.Path;
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
    public final ConcurrentHashMap<String, Path> jarStorage;
    public final ConcurrentHashMap<String, Path> jobStorage;
    public final Path jarStorageDir;
    public final Path jobStorageDir;
    public final int port;
    public final ConcurrentHashMap<Pair<String, String>, WorkerTaskStatus> statusMap;
    public final ConcurrentHashMap<Pair<String, String>, TaskResult> mapTaskResults = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Pair<String, String>, ReduceTaskResult> reduceTaskResults = new ConcurrentHashMap<>();

    public WorkerContext(IntermediateStorage intermediateStorage, String workerId, Path jarStorageDir, Path jobStorageDir, int port) {
        this.intermediateStorage = intermediateStorage;
        this.workerId = workerId;
        this.jarStorage = new ConcurrentHashMap<>();
        this.statusMap = new ConcurrentHashMap<>();
        this.jobStorage = new ConcurrentHashMap<>();
        this.jarStorageDir = jarStorageDir;
        this.jobStorageDir = jobStorageDir;
        this.port = port;
    }

}
