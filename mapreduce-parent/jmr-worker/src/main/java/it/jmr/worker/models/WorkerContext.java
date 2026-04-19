package it.jmr.worker.models;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.utils.Pair;
import it.jmr.worker.storage.IntermediateStorage;
import it.jmr.worker.storage.ReduceResultStorage;

/**
 * Represents the shared data and state of a worker node in the MapReduce
 * framework.
 */
public class WorkerContext {
    private static final int MAX_EVENTS = 200;

    public record DashboardEvent(long timestamp, String message) {
    }

    private final AtomicBoolean busy = new AtomicBoolean(false);
    public final IntermediateStorage intermediateStorage;
    public final ReduceResultStorage reduceResultStorage;
    public final String workerId;
    public final ConcurrentHashMap<String, Path> jarStorage;
    public final ConcurrentHashMap<String, Path> jobStorage;
    public final ConcurrentHashMap<String, String> jobJarIds;
    public final Path jarStorageDir;
    public final Path jobStorageDir;
    public final int port;
    public final ConcurrentHashMap<Pair<String, String>, WorkerTaskStatus> statusMap;
    public final ConcurrentHashMap<Pair<String, String>, TaskResult> mapTaskResults = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Pair<String, String>, ReduceTaskResult> reduceTaskResults = new ConcurrentHashMap<>();
    public final ExecutorService taskExecutor;
    private final Deque<DashboardEvent> dashboardEvents;
    private final Deque<DashboardEvent> logEvents;

    public WorkerContext(IntermediateStorage intermediateStorage, ReduceResultStorage reduceResultStorage, String workerId, Path jarStorageDir,
            Path jobStorageDir, int port) {
        this.intermediateStorage = intermediateStorage;
        this.reduceResultStorage = reduceResultStorage;
        this.workerId = workerId;
        this.jarStorage = new ConcurrentHashMap<>();
        this.statusMap = new ConcurrentHashMap<>();
        this.jobStorage = new ConcurrentHashMap<>();
        this.jobJarIds = new ConcurrentHashMap<>();
        this.jarStorageDir = jarStorageDir;
        this.jobStorageDir = jobStorageDir;
        this.port = port;
        this.taskExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread thread = new Thread(runnable, "worker-task-" + workerId);
                thread.setDaemon(true);
                return thread;
            }
        });
        this.dashboardEvents = new ArrayDeque<>();
        this.logEvents = new ArrayDeque<>();
    }

    public boolean tryAcquireTaskSlot() {
        return busy.compareAndSet(false, true);
    }

    public void releaseTaskSlot() {
        busy.set(false);
    }

    public boolean isBusy() {
        return busy.get();
    }

    public void rememberJobJar(final String jobId, final String jarId) {
        if (jobId != null && jarId != null && !jarId.isBlank()) {
            jobJarIds.putIfAbsent(jobId, jarId);
        }
    }

    public String forgetJobJar(final String jobId) {
        return jobJarIds.remove(jobId);
    }

    public boolean isJarStillReferenced(final String jarId) {
        return jobJarIds.containsValue(jarId);
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

    public synchronized void recordLog(final String message) {
        if (logEvents.size() >= MAX_EVENTS) {
            logEvents.removeFirst();
        }
        logEvents.addLast(new DashboardEvent(System.currentTimeMillis(), message));
    }

    public synchronized List<DashboardEvent> getLogEvents() {
        return new ArrayList<>(logEvents);
    }

    public void clearRuntimeState() {
        releaseTaskSlot();
        taskExecutor.shutdownNow();
        statusMap.clear();
        mapTaskResults.clear();
        reduceTaskResults.clear();
        jarStorage.clear();
        jobStorage.clear();
        jobJarIds.clear();
        intermediateStorage.clear();
        reduceResultStorage.clear();
        synchronized (this) {
            dashboardEvents.clear();
            logEvents.clear();
        }
    }

}
