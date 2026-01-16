package it.jmr.master;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

public class MasterContext {
    public final int port;
    public final Path jarStorageDir;
    public final Path jobStorageDir;
    public final Path rootStoragePath;
    public final List<Worker> workers;
    public final ConcurrentHashMap<String, String> jarsPaths;
    public final ConcurrentHashMap<String, String> jobsPaths;
    public final Queue<JobInfoInternal> jobQueue;

    public MasterContext(int port, Path rootStoragePath, Path jarStorageDirectory, Path jobStorageDirectory, List<WorkerI> workers) {
        this.port = port;
        this.rootStoragePath = rootStoragePath;
        this.jarStorageDir = jarStorageDirectory;
        this.jobStorageDir = jobStorageDirectory;
        this.jobQueue = new ConcurrentLinkedDeque<>();
        this.jarsPaths = new ConcurrentHashMap<>();
        this.jobsPaths = new ConcurrentHashMap<>();
        this.workers = (workers.stream().map(Worker::new).collect(Collectors.toCollection(CopyOnWriteArrayList::new)));
    }

}
