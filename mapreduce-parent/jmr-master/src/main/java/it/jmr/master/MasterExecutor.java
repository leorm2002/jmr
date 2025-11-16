package it.jmr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.jarservice.JobClassLoader;
import it.jmr.common.models.IntermediateLocation;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.JobStatus;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

public class MasterExecutor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterExecutor.class);

    public record UnassignedMapTasks(String jobId, String taskId, Pair<Long, Long> chunks, String jarId) {
        public AssignedMapTasks toAssigned(Worker worker) {
            return new AssignedMapTasks(worker, jobId, taskId, chunks, jarId);
        }
    }

    public record AssignedMapTasks(Worker worker, String jobId, String taskId, Pair<Long, Long> chunks, String jarId) {
        CompletedMapTasks toCompleted(List<IntermediateLocation> intermediateDataLocations) {
            return new CompletedMapTasks(worker, jobId, taskId, chunks, jarId, intermediateDataLocations);
        }

        UnassignedMapTasks toUnassigned() {
            return new UnassignedMapTasks(jobId, taskId, chunks, jarId);
        }
    }

    public record CompletedMapTasks(Worker worker, String jobId, String taskId, Pair<Long, Long> chunks, String jarId,
            List<IntermediateLocation> intermediateDataLocations) {
    }

    private final MasterContext ctx;

    public MasterExecutor(MasterContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void run() {
        while (true) {
            final JobInfoInternal job = ctx.jobQueue.poll();

            if (job != null) {
                try {
                    executeJobNew(job, ctx.workers);
                } catch (Exception e) {
                    JMRLog.error(LOGGER, "Error executing job {}: {}", job.getJobId(), e.getMessage());
                }
            }

            // Sleep or wait for new jobs
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> void executeJobNew(
            JobInfoInternal jobInfo, List<Worker> workers) {

        LOGGER.debug("\n>>> Avvio esecuzione job: " + jobInfo.getJobId());
        jobInfo.setStatus(JobStatus.RUNNING);

        // 1st step: load the job configuration
        final String jobJarPath = jobInfo.getJarPath();
        final String jobSerializedPath = jobInfo.getJobPath();

        final JobConfiguration<D, V, O> jobConfig;

        try {
            jobConfig = new JobClassLoader(jobJarPath, jobSerializedPath).deserializeFromFile();
        } catch (Exception e) {
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage(e.getMessage());
            JMRLog.error(LOGGER, "<<< Job failder: {}", jobInfo.getJobId());
            JMRLog.error(LOGGER, "Error: {}", e.getMessage());
            JMRLog.error(LOGGER, "Error: {}", e.toString());
            JMRLog.error(LOGGER, "StackTrace: {}", (e.getCause() != null ? e.getCause().getStackTrace() : e.getStackTrace()));

            JMRLog.error(LOGGER, "Errore completo di deserializzazione:", e);

            throw new RuntimeException("Error loading job configuration: " + e.getMessage(), e);

        }

        // 2nd step: fetch data size
        final long dataSize;

        try {
            final DataProviderClient<D> provider = jobConfig.getDataProvider();
            provider.init();
            dataSize = provider.size();
        } catch (Exception e) {
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage(e.getMessage());
            JMRLog.error(LOGGER, "<<< Job fallito: {}", jobInfo.getJobId());
            JMRLog.error(LOGGER, "Error loading job configuration: {}", e.getMessage());
            throw new RuntimeException("Error loading job configuration: " + e.getMessage(), e);
        }

        // 3rd step divide the works in map tasks
        final int n = workers.size() * 3;
        final long chunkSize = dataSize / n;
        final List<Pair<Long, Long>> chunks = buildChunks(dataSize, chunkSize);
        JMRLog.info(LOGGER, "Job {}: data size = {}, divided in {} chunks", jobInfo.getJobId(), dataSize, chunks.size());

        final List<UnassignedMapTasks> mapTasks = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            final UnassignedMapTasks task = new UnassignedMapTasks(jobInfo.getJobId(), jobInfo.getJobId() + "-map-" + i, chunks.get(i), jobJarPath);
            mapTasks.add(task);
        }

        // Mapping

        // Liste e code per l'assegnazione e il monitoraggio dei task
        final List<AssignedMapTasks> assignedTasks = Collections.synchronizedList(new ArrayList<>());
        final List<CompletedMapTasks> completedTasks = Collections.synchronizedList(new ArrayList<>());
        final Queue<UnassignedMapTasks> unassignedQueue = new ConcurrentLinkedQueue<>(mapTasks);
        final AtomicBoolean stopForError = new AtomicBoolean(false);

        // Submit task assigner
        final var assignerFuture = ExecutorManager.getExecutor()
                .submit(() -> submitUnassignedTasks(workers, assignedTasks, unassignedQueue, stopForError, jobConfig));

        // Submit task monitor
        final var monitorFuture = ExecutorManager.getExecutor()
                .submit(() -> monitorAssignedTasks(mapTasks, assignedTasks, completedTasks, unassignedQueue, stopForError));

        try {
            assignerFuture.get();
            monitorFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopForError.set(true);
        } catch (Exception e) {
            stopForError.set(true);
            throw new RuntimeException("Error during job execution", e);
        }

        JMRLog.debug(LOGGER, "FASE DI MAP TERMINATA");
        JMRLog.debug(LOGGER, "DEFINIZIONE DI PARTITIONING E SHUFFLING DEI DATI");

        final Map<String, List<IntermediateLocation>> partitions = completedTasks.stream().map(c -> c.intermediateDataLocations).flatMap(List::stream)
                .collect(Collectors.groupingBy(IntermediateLocation::getPartitionId));

        for (var partition : partitions.entrySet()) {
            JMRLog.debug(LOGGER, "Partizione {}: {} elementi", partition.getKey(), partition.getValue().size());
            // Creo un task di reduce per questa partizione
            // Task composto da:
            // - id del job
            // - id del task di riduzione
            // - lista delle locazioni intermedie da cui prelevare i dati
            // - jarId

        }

        JMRLog.debug(LOGGER, "INIZIO FASE DI REDUCE");

        // Reduction phase (not implemented)

        System.out.println("--- Output del job ---");
        // System.out.println(result);
        System.out.println("--- Fine output ---");

        jobInfo.setStatus(JobStatus.COMPLETED);
        System.out.println("<<< Job completato: " + jobInfo.getJobId() + " (tempo: " + jobInfo.getExecutionTime() + "ms)\n");

    }

    private static void monitorAssignedTasks(final List<UnassignedMapTasks> mapTasks, final List<AssignedMapTasks> assignedTasks,
            final List<CompletedMapTasks> completedTasks, final Queue<UnassignedMapTasks> unassignedQueue, final AtomicBoolean stopForError) {
        // Continua finché non sono stati completati tutti i task o non si è verificato
        // un errore
        while (!stopForError.get() && completedTasks.size() < mapTasks.size()) {
            JMRLog.debug(LOGGER, "Monitor dei task assegnati: {} completati su {}", completedTasks.size(), mapTasks.size());
            final List<AssignedMapTasks> toRemove = new ArrayList<>();

            for (final AssignedMapTasks assigned : assignedTasks) {
                final Pair<WorkerTaskStatus, List<IntermediateLocation>> statusResponse = assigned.worker().getMapTaskStatus(assigned.jobId(),
                        assigned.taskId());
                final WorkerTaskStatus status = statusResponse.getFirst();

                if (status == WorkerTaskStatus.COMPLETED) {
                    completedTasks.add(assigned.toCompleted(statusResponse.getSecond()));
                    toRemove.add(assigned);
                    JMRLog.debug(LOGGER, "Task {} completato", assigned.taskId());
                } else if (status == WorkerTaskStatus.FAILED || status == WorkerTaskStatus.MISSING) {
                    // Rimetti il task nella coda degli unassigned
                    unassignedQueue.offer(assigned.toUnassigned());
                    toRemove.add(assigned);
                    JMRLog.debug(LOGGER, "Task {} fallito o mancante, rimesso in coda", assigned.taskId());
                } else {
                    JMRLog.debug(LOGGER, "Task {} ancora in esecuzione", assigned.taskId());
                }
            }

            assignedTasks.removeAll(toRemove);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopForError.set(true);
                break;
            }
        }
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedTasks(List<Worker> workers,
            final List<AssignedMapTasks> assignedTasks, final Queue<UnassignedMapTasks> unassignedQueue, final AtomicBoolean stopForError,
            JobConfiguration<D, V, O> jobConfig) {
        // Continua finché ci sono task da assegnare o task in esecuzione e non è stato
        // segnalato un errore
        while (!(unassignedQueue.isEmpty() && assignedTasks.isEmpty()) && !stopForError.get()) {
            final UnassignedMapTasks task = unassignedQueue.poll();
            if (task == null) {
                continue;
            }
            JMRLog.debug(LOGGER, "Avvio processo di assegnazione task {}", task.taskId());

            // Prova ad assegnare il task, può non riuscirci se tutti i worker sono occupati
            final Optional<AssignedMapTasks> assigned = tryAssignMapTask(task, workers, assignedTasks, jobConfig);

            if (assigned.isPresent()) {
                JMRLog.debug(LOGGER, "Task {} assegnato con successo a {}", task.taskId(),
                        assigned.get().worker().getAddress() + ":" + assigned.get().worker().getPort());
                // Task assegnato con successo
                assignedTasks.add(assigned.get());

            } else {
                JMRLog.debug(LOGGER, "Nessun worker disponibile per il task {}", task.taskId());
                // Nessun worker disponibile, rimetti in coda
                unassignedQueue.offer(task);

                // Attendi un po' prima di riprovare
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopForError.set(true);
                    break;
                }
            }

        }
    }

    private static List<Pair<Long, Long>> buildChunks(long dataSize, long chunkSize) {
        List<Pair<Long, Long>> chunks = new ArrayList<>();
        long start = 0;
        while (start < dataSize) {
            long end = Math.min(start + chunkSize, dataSize);
            chunks.add(new Pair<>(start, end));
            start = end;
        }
        return chunks;
    }

    /**
     * Attempt to assign a map task to an available worker
     */
    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedMapTasks> tryAssignMapTask(
            UnassignedMapTasks task, List<Worker> workers, List<AssignedMapTasks> assignedTasks, JobConfiguration<D, V, O> jobConfig) {
        final Set<Worker> busy = assignedTasks.stream().map(AssignedMapTasks::worker).collect(Collectors.toSet());
        for (final Worker worker : workers) {
            if (busy.contains(worker)) {
                continue;
            }

            final Optional<AssignedMapTasks> result = tryAssignMapTask(task, worker, jobConfig);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedMapTasks> tryAssignMapTask(
            UnassignedMapTasks task, Worker worker, JobConfiguration<D, V, O> jobConfig) {
        final boolean assigned = worker.submitMapTask(task.jobId(), task.taskId(), task.chunks().getFirst().intValue(),
                (int) (task.chunks().getSecond() - task.chunks().getFirst()), task.jarId(), jobConfig);
        if (assigned) {
            return Optional.of(task.toAssigned(worker));
        }
        return Optional.empty();
    }
}
