package it.jmr.worker.models;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.utils.Pair;
import it.jmr.worker.storage.IntermediateStorage;

/**
 * Represents the shared data and state of a worker node in the MapReduce
 * framework.
 */
public class WorkerContext {
    private static final int MAX_EVENTS = 200;

    public record DashboardEvent(long timestamp, String message) {
    }

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
    private final Deque<DashboardEvent> dashboardEvents;

    public WorkerContext(IntermediateStorage intermediateStorage, String workerId, Path jarStorageDir, Path jobStorageDir, int port) {
        this.intermediateStorage = intermediateStorage;
        this.workerId = workerId;
        this.jarStorage = new ConcurrentHashMap<>();
        this.statusMap = new ConcurrentHashMap<>();
        this.jobStorage = new ConcurrentHashMap<>();
        this.jarStorageDir = jarStorageDir;
        this.jobStorageDir = jobStorageDir;
        this.port = port;
        this.dashboardEvents = new ArrayDeque<>();
    }

    public synchronized void recordEvent(final String message) {
        if (dashboardEvents.size() >= MAX_EVENTS) {
            dashboardEvents.removeFirst();
        }
        dashboardEvents.addLast(new DashboardEvent(System.currentTimeMillis(), message));
    }

    public synchronized List<DashboardEvent> getDashboardEvents() {
        return new ArrayList<>(dashboardEvents);
    }

    public void clearRuntimeState() {
        busy = false;
        statusMap.clear();
        mapTaskResults.clear();
        reduceTaskResults.clear();
        jarStorage.clear();
        jobStorage.clear();
        intermediateStorage.clear();
        synchronized (this) {
            dashboardEvents.clear();
        }
    }

}
