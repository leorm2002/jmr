package it.jmr.master;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import it.jmr.master.events.WorkerFailureListener;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

public class MasterContext {
    private static final int MAX_EVENTS = 200;

    public record DashboardEvent(long timestamp, String message) {
    }

    public final int port;
    /** Represents the folder where JAR files are stored */
    public final Path jarStorageDir;
    /** Represents the folder where job-related files are stored */
    public final Path jobStorageDir;
    /** Represents the root storage path for the master */
    public final Path rootStoragePath;
    /** Represents the list of workers available to the master */
    public final List<Worker> workers;
    public final ConcurrentHashMap<String, Path> jarsPaths;
    public final ConcurrentHashMap<String, Path> jobsPaths;
    public final ConcurrentHashMap<String, JobInfoInternal> jobs;
    public final Queue<JobInfoInternal> jobQueue;
    private final List<WorkerFailureListener> workerFailureListeners;
    private final Deque<DashboardEvent> dashboardEvents;
    private volatile JobInfoInternal activeJob;
    private volatile MasterExecutor.JobExecutionContext<?> activeJobContext;
    private volatile String activePhase;

    public MasterContext(int port, Path rootStoragePath, Path jarStorageDirectory, Path jobStorageDirectory, List<WorkerI> workers) {
        this.port = port;
        this.rootStoragePath = rootStoragePath;
        this.jarStorageDir = jarStorageDirectory;
        this.jobStorageDir = jobStorageDirectory;
        this.jobQueue = new ConcurrentLinkedDeque<>();
        this.jarsPaths = new ConcurrentHashMap<>();
        this.jobsPaths = new ConcurrentHashMap<>();
        this.jobs = new ConcurrentHashMap<>();
        this.workers = (workers.stream().map(Worker::new).collect(Collectors.toCollection(CopyOnWriteArrayList::new)));
        this.workerFailureListeners = new CopyOnWriteArrayList<>();
        this.dashboardEvents = new ArrayDeque<>();
        this.activePhase = "IDLE";
    }

    public void addWorkerFailureListener(WorkerFailureListener listener) {
        workerFailureListeners.add(listener);
    }

    public void removeWorkerFailureListener(WorkerFailureListener listener) {
        workerFailureListeners.remove(listener);
    }

    public void notifyWorkerFailed(Worker worker) {
        for (WorkerFailureListener listener : workerFailureListeners) {
            listener.onWorkerFailed(worker);
        }
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

    public void setActiveExecution(final JobInfoInternal jobInfo, final MasterExecutor.JobExecutionContext<?> jobContext, final String phase) {
        this.activeJob = jobInfo;
        this.activeJobContext = jobContext;
        this.activePhase = phase;
    }

    public void updateActivePhase(final String phase) {
        this.activePhase = phase;
    }

    public void clearActiveExecution() {
        this.activeJob = null;
        this.activeJobContext = null;
        this.activePhase = "IDLE";
    }

    public JobInfoInternal getActiveJob() {
        return activeJob;
    }

    public MasterExecutor.JobExecutionContext<?> getActiveJobContext() {
        return activeJobContext;
    }

    public String getActivePhase() {
        return activePhase;
    }

    public void clearRuntimeState() {
        jobQueue.clear();
        jobs.clear();
        jobsPaths.clear();
        jarsPaths.clear();
        workerFailureListeners.clear();
        clearActiveExecution();
        synchronized (this) {
            dashboardEvents.clear();
        }
    }
}
