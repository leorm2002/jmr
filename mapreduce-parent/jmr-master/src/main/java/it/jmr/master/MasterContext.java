package it.jmr.master;

import java.nio.file.Path;
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
    public final Queue<JobInfoInternal> jobQueue;
    private final List<WorkerFailureListener> workerFailureListeners;

    public MasterContext(int port, Path rootStoragePath, Path jarStorageDirectory, Path jobStorageDirectory, List<WorkerI> workers) {
        this.port = port;
        this.rootStoragePath = rootStoragePath;
        this.jarStorageDir = jarStorageDirectory;
        this.jobStorageDir = jobStorageDirectory;
        this.jobQueue = new ConcurrentLinkedDeque<>();
        this.jarsPaths = new ConcurrentHashMap<>();
        this.jobsPaths = new ConcurrentHashMap<>();
        this.workers = (workers.stream().map(Worker::new).collect(Collectors.toCollection(CopyOnWriteArrayList::new)));
        this.workerFailureListeners = new CopyOnWriteArrayList<>();
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
}
