package it.jmr.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

public class MasterContext {
    public final int port;
    public final String jarStorageDir;
    public final String jobStorageDir;
    public final List<Worker> workers;
    public final ConcurrentHashMap<String, String> jarsPaths;
    public final ConcurrentHashMap<String, String> jobsPaths;
    public final Queue<JobInfoInternal> jobQueue;

    public MasterContext(int port, String jarStorageDir, String jobStorageDir, List<WorkerI> workers) {
        this.port = port;
        this.jarStorageDir = jarStorageDir;
        this.jobStorageDir = jobStorageDir;
        this.jobQueue = new ConcurrentLinkedDeque<>();
        this.jarsPaths = new ConcurrentHashMap<>();
        this.jobsPaths = new ConcurrentHashMap<>();
        this.workers = Collections.synchronizedList(workers.stream().map(Worker::new).collect(Collectors.toCollection(ArrayList::new)));
    }

}
