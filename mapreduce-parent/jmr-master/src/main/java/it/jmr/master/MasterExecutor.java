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

import it.jmr.common.JMRConstants;
import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.jarservice.JobClassLoader;
import it.jmr.common.models.IntermediateLocation;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.ExecutorManager;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.JmrUtils;
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

            JmrUtils.sleep(JMRConstants.MASTER_POLL_INTERVAL_MS);
        }
    }

    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> void executeJobNew(
            JobInfoInternal jobInfo, List<Worker> workers) throws JMRException {

        LOGGER.info("\n>>> Starting job execution: {}", jobInfo.getJobId());
        jobInfo.setStatus(JobStatus.RUNNING);

        final String jobJarPath = jobInfo.getJarPath();
        final String jobSerializedPath = jobInfo.getJobPath();

        final JobConfiguration<D, V, O> jobConfig;

        try {
            jobConfig = new JobClassLoader(jobJarPath, jobSerializedPath).deserializeFromFile();
        } catch (Exception e) {
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage(e.getMessage());
            JMRLog.error(LOGGER, "<<< Job failed: {}", jobInfo.getJobId());
            JMRLog.error(LOGGER, "Error: {}", e.getMessage());
            JMRLog.error(LOGGER, "Error: {}", e.toString());
            JMRLog.error(LOGGER, "StackTrace: {}", (e.getCause() != null ? e.getCause().getStackTrace() : e.getStackTrace()));

            JMRLog.error(LOGGER, "Full deserialization error:", e);

            throw new JMRException("Error loading job configuration: " + e.getMessage(), e);

        }

        final long dataSize;

        try {
            final DataProviderClient<D> provider = jobConfig.getDataProvider();
            provider.init();
            dataSize = provider.size();
        } catch (Exception e) {
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage(e.getMessage());
            JMRLog.error(LOGGER, "<<< Job failed: {}", jobInfo.getJobId());
            JMRLog.error(LOGGER, "Error loading job configuration: {}", e.getMessage());
            throw new JMRException("Error loading job configuration: " + e.getMessage(), e);
        }

        final int n = workers.size() * 3;
        final long chunkSize = dataSize / n;
        final List<Pair<Long, Long>> chunks = buildChunks(dataSize, chunkSize);
        JMRLog.info(LOGGER, "Job {}: data size = {}, divided in {} chunks", jobInfo.getJobId(), dataSize, chunks.size());

        final List<UnassignedMapTasks> mapTasks = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            final UnassignedMapTasks task = new UnassignedMapTasks(jobInfo.getJobId(), jobInfo.getJobId() + "-map-" + i, chunks.get(i), jobJarPath);
            mapTasks.add(task);
        }

        final List<AssignedMapTasks> assignedTasks = Collections.synchronizedList(new ArrayList<>());
        final List<CompletedMapTasks> completedTasks = Collections.synchronizedList(new ArrayList<>());
        final Queue<UnassignedMapTasks> unassignedQueue = new ConcurrentLinkedQueue<>(mapTasks);
        final AtomicBoolean stopForError = new AtomicBoolean(false);

        final var assignerFuture = ExecutorManager.getExecutor()
                .submit(() -> submitUnassignedTasks(workers, assignedTasks, unassignedQueue, stopForError, jobConfig));

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
            throw new JMRException("Error during job execution", e);
        }

        JMRLog.debug(LOGGER, "MAP PHASE COMPLETED");
        JMRLog.debug(LOGGER, "PARTITIONING AND SHUFFLING DATA");

        final Map<String, List<IntermediateLocation>> partitions = completedTasks.stream().map(c -> c.intermediateDataLocations).flatMap(List::stream)
                .collect(Collectors.groupingBy(IntermediateLocation::getPartitionId));

        JMRLog.debug(LOGGER, "STARTING REDUCE PHASE");

        jobInfo.setStatus(JobStatus.COMPLETED);
        LOGGER.info("<<< Job completed: {} (time: {}ms)\n", jobInfo.getJobId(), jobInfo.getExecutionTime());
    }

    private static void monitorAssignedTasks(final List<UnassignedMapTasks> mapTasks, final List<AssignedMapTasks> assignedTasks,
            final List<CompletedMapTasks> completedTasks, final Queue<UnassignedMapTasks> unassignedQueue, final AtomicBoolean stopForError) {
        while (!stopForError.get() && completedTasks.size() < mapTasks.size()) {
            JMRLog.debug(LOGGER, "Monitoring assigned tasks: {} completed out of {}", completedTasks.size(), mapTasks.size());
            final List<AssignedMapTasks> toRemove = new ArrayList<>();

            for (final AssignedMapTasks assigned : assignedTasks) {
                final Pair<WorkerTaskStatus, List<IntermediateLocation>> statusResponse = assigned.worker().getMapTaskStatus(assigned.jobId(),
                        assigned.taskId());
                final WorkerTaskStatus status = statusResponse.getFirst();

                if (status == WorkerTaskStatus.COMPLETED) {
                    completedTasks.add(assigned.toCompleted(statusResponse.getSecond()));
                    toRemove.add(assigned);
                    JMRLog.debug(LOGGER, "Task {} completed", assigned.taskId());
                } else if (status == WorkerTaskStatus.FAILED || status == WorkerTaskStatus.MISSING) {
                    unassignedQueue.offer(assigned.toUnassigned());
                    toRemove.add(assigned);
                    JMRLog.warn(LOGGER, "Task {} failed or missing, re-queuing", assigned.taskId());
                } else {
                    JMRLog.trace(LOGGER, "Task {} still running", assigned.taskId());
                }
            }

            assignedTasks.removeAll(toRemove);

            try {
                Thread.sleep(JMRConstants.MAP_TASK_MONITOR_SLEEP_MS);
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
        while (!(unassignedQueue.isEmpty() && assignedTasks.isEmpty()) && !stopForError.get()) {
            final UnassignedMapTasks task = unassignedQueue.poll();
            if (task == null) {
                continue;
            }
            JMRLog.debug(LOGGER, "Starting task assignment process for {}", task.taskId());

            final Optional<AssignedMapTasks> assigned = tryAssignMapTask(task, workers, assignedTasks, jobConfig);

            if (assigned.isPresent()) {
                JMRLog.debug(LOGGER, "Task {} assigned successfully to {}", task.taskId(),
                        assigned.get().worker().getAddress() + ":" + assigned.get().worker().getPort());
                assignedTasks.add(assigned.get());

            } else {
                JMRLog.debug(LOGGER, "No worker available for task {}", task.taskId());
                unassignedQueue.offer(task);

                try {
                    Thread.sleep(JMRConstants.MAP_TASK_SCHEDULER_SLEEP_MS);
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
