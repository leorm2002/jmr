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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
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

    public record UnassignedReduceTasks(String jobId, String taskId, String jarId, String partitionId,
            List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations) {
        public AssignedReduceTasks toAssigned(Worker worker) {
            return new AssignedReduceTasks(worker, jobId, taskId, jarId, partitionId, intermediateDataLocations);
        }
    }

    public record AssignedReduceTasks(Worker worker, String jobId, String taskId, String jarId, String partitionId,
            List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations) {
        <O extends Serializable> CompletedReduceTasks<O> toCompleted(List<Pair<String, O>> reducedData) {
            return new CompletedReduceTasks<>(worker, jobId, taskId, jarId, partitionId, intermediateDataLocations, reducedData);
        }

        UnassignedReduceTasks toUnassigned() {
            return new UnassignedReduceTasks(jobId, taskId, jarId, partitionId, intermediateDataLocations);
        }
    }

    public record CompletedReduceTasks<O extends Serializable>(Worker worker, String jobId, String taskId, String jarId, String partitionId,
            List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations, List<Pair<String, O>> reducedData) {
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
                    executeJob(job, ctx.workers);
                } catch (Exception e) {
                    JMRLog.error(LOGGER, "Error executing job {}: {}", job.getJobId(), e.getMessage());
                }
            }

            JmrUtils.sleep(JMRConstants.MASTER_POLL_INTERVAL_MS);
        }
    }

    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> void executeJob(
            JobInfoInternal jobInfo, List<Worker> workers) throws JMRException {

        LOGGER.info("\n>>> Starting job execution: {}", jobInfo.getJobId());
        jobInfo.setStatus(JobStatus.RUNNING);

        final String jarId = jobInfo.getJarId();
        final String jobId = jobInfo.getJobId();
        final String jobSerializedPath = jobInfo.getJobPath();

        // Desrialize job configuration
        final JobConfiguration<D, V, O> jobConfig = deserializeJobConfig(jobInfo, jobInfo.getJarPath(), jobSerializedPath);

        // Loading expected data size to proper partition
        final long dataSize = getDataSize(jobInfo, jobConfig);

        // Partition data into separate map tasks
        final List<UnassignedMapTasks> mapTasks = chunkIntoMapTasks(workers, jarId, jobId, dataSize);

        final List<AssignedMapTasks> assignedTasks = new CopyOnWriteArrayList<>();
        final List<CompletedMapTasks> completedTasks = new CopyOnWriteArrayList<>();
        final Queue<UnassignedMapTasks> unassignedQueue = new ConcurrentLinkedQueue<>(mapTasks);
        final AtomicBoolean stopForError = new AtomicBoolean(false);

        final Future<?> assignerFuture = ExecutorManager.getExecutor()
                .submit(() -> submitUnassignedTasks(workers, assignedTasks, unassignedQueue, stopForError, jobConfig));

        final Future<?> monitorFuture = ExecutorManager.getExecutor()
                .submit(() -> monitorAssignedMapTasks(mapTasks, assignedTasks, completedTasks, unassignedQueue, stopForError));

        executeMapping(jobInfo, stopForError, assignerFuture, monitorFuture);

        JMRLog.debug(LOGGER, "MAP PHASE COMPLETED");

        JMRLog.debug(LOGGER, "PARTITIONING AND SHUFFLING DATA");

        final Map<String, List<IntermediateLocation>> partitions = completedTasks.stream().map(c -> c.intermediateDataLocations).flatMap(List::stream)
                .collect(Collectors.groupingBy(IntermediateLocation::getPartitionId));

        JMRLog.debug(LOGGER, "STARTING REDUCE PHASE");

        final List<UnassignedReduceTasks> reduceTasks = createReduceTasks(jobInfo, jarId, partitions).stream().limit(300).toList();

        final List<AssignedReduceTasks> assignedReduceTasks = new CopyOnWriteArrayList<>();
        final List<CompletedReduceTasks<O>> completedReduceTasks = new CopyOnWriteArrayList<>();
        final Queue<UnassignedReduceTasks> unassignedReduceQueue = new ConcurrentLinkedQueue<>(reduceTasks);
        stopForError.set(false); // Reset for reduce phase

        final Future<?> reduceAssignerFuture = ExecutorManager.getExecutor()
                .submit(() -> submitUnassignedReduceTasks(workers, assignedReduceTasks, unassignedReduceQueue, stopForError, jobConfig));

        final Future<?> reduceMonitorFuture = ExecutorManager.getExecutor().submit(
                () -> monitorAssignedReduceTasks(reduceTasks, assignedReduceTasks, completedReduceTasks, unassignedReduceQueue, stopForError));

        reduce(jobInfo, stopForError, reduceAssignerFuture, reduceMonitorFuture);

        JMRLog.debug(LOGGER, "REDUCE PHASE COMPLETED");

        // Collect final results (for now, just log them)
        for (final CompletedReduceTasks<O> task : completedReduceTasks) {
            JMRLog.info(LOGGER, "Reduced data for partition {}: {}", task.partitionId(), task.reducedData());
        }

        jobInfo.setStatus(JobStatus.COMPLETED);
        LOGGER.info("<<< Job completed: {} (time: {}ms)\n", jobInfo.getJobId(), jobInfo.getExecutionTime());
    }

    private static void reduce(JobInfoInternal jobInfo, final AtomicBoolean stopForError, final Future<?> reduceAssignerFuture,
            final Future<?> reduceMonitorFuture) throws JMRException {
        try {
            reduceAssignerFuture.get();
            reduceMonitorFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopForError.set(true);
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage("Reduce phase interrupted");
            throw new JMRException("Reduce phase interrupted", e);
        } catch (Exception e) {
            stopForError.set(true);
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage("Error during reduce phase: " + e.getMessage());
            throw new JMRException("Error during reduce phase", e);
        }
    }

    private static List<UnassignedReduceTasks> createReduceTasks(JobInfoInternal jobInfo, final String jarId,
            final Map<String, List<IntermediateLocation>> partitions) {
        final List<UnassignedReduceTasks> reduceTasks = new ArrayList<>();

        for (Map.Entry<String, List<IntermediateLocation>> partition : partitions.entrySet()) {
            final String partitionId = partition.getKey();
            final List<IntermediateLocation> intermediateLocations = partition.getValue();

            final List<it.jmr.grpc.worker.IntermediateDataLocation> locations = intermediateLocations
                    .stream().map(loc -> it.jmr.grpc.worker.IntermediateDataLocation.newBuilder().setWorkerId(loc.getWorkerId())
                            .setTaskId(loc.getTaskId()).setPartitionId(loc.getPartitionId()).setHost(loc.getHost()).setPort(loc.getPort()).build())
                    .collect(Collectors.toList());
            final UnassignedReduceTasks task = new UnassignedReduceTasks(jobInfo.getJobId(), jobInfo.getJobId() + "-reduce-" + partitionId, jarId,
                    partitionId, locations);
            reduceTasks.add(task);
        }
        return reduceTasks;
    }

    private static void executeMapping(JobInfoInternal jobInfo, final AtomicBoolean stopForError, final Future<?> assignerFuture,
            final Future<?> monitorFuture) throws JMRException {
        try {
            assignerFuture.get();
            monitorFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stopForError.set(true);
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage("Map phase interrupted");
            throw new JMRException("Map phase interrupted", e);
        } catch (Exception e) {
            stopForError.set(true);
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage("Error during map phase: " + e.getMessage());
            throw new JMRException("Error during map phase", e);
        }
    }

    private static List<UnassignedMapTasks> chunkIntoMapTasks(List<Worker> workers, final String jarId, final String jobId, final long dataSize) {
        final List<UnassignedMapTasks> mapTasks = new ArrayList<>();

        final int n = workers.size() * 3;
        final long chunkSize = dataSize / n;
        final List<Pair<Long, Long>> chunks = buildChunks(dataSize, chunkSize);
        JMRLog.info(LOGGER, "Job {}: data size = {}, divided in {} chunks", jobId, dataSize, chunks.size());

        for (int i = 0; i < chunks.size(); i++) {
            final UnassignedMapTasks task = new UnassignedMapTasks(jobId, jobId + "-map-" + i, chunks.get(i), jarId);
            mapTasks.add(task);
        }

        return mapTasks;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> long getDataSize(JobInfoInternal jobInfo,
            final JobConfiguration<D, V, O> jobConfig) throws JMRException {
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
        return dataSize;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> JobConfiguration<D, V, O> deserializeJobConfig(
            JobInfoInternal jobInfo, final String jobJarPath, final String jobSerializedPath) throws JMRException {

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
        return jobConfig;
    }

    private static <O extends Serializable> void monitorAssignedReduceTasks(final List<UnassignedReduceTasks> reduceTasks,
            final List<AssignedReduceTasks> assignedReduceTasks, final List<CompletedReduceTasks<O>> completedReduceTasks,
            final Queue<UnassignedReduceTasks> unassignedReduceQueue, final AtomicBoolean stopForError) {
        while (!stopForError.get() && completedReduceTasks.size() < reduceTasks.size()) {
            JMRLog.debug(LOGGER, "Monitoring assigned reduce tasks: {} completed out of {}", completedReduceTasks.size(), reduceTasks.size());
            final List<AssignedReduceTasks> toRemove = new ArrayList<>();

            for (final AssignedReduceTasks assigned : assignedReduceTasks) {
                final Pair<WorkerTaskStatus, List<Pair<String, O>>> statusResponse = assigned.worker().getReduceTaskStatus(assigned.jobId(),
                        assigned.taskId());
                final WorkerTaskStatus status = statusResponse.getFirst();

                if (status == WorkerTaskStatus.COMPLETED) {
                    completedReduceTasks.add(assigned.toCompleted(statusResponse.getSecond()));
                    toRemove.add(assigned);
                    JMRLog.debug(LOGGER, "Reduce task {} completed", assigned.taskId());
                } else if (status == WorkerTaskStatus.FAILED || status == WorkerTaskStatus.MISSING) {
                    unassignedReduceQueue.offer(assigned.toUnassigned());
                    toRemove.add(assigned);
                    JMRLog.warn(LOGGER, "Reduce task {} failed or missing, re-queuing", assigned.taskId());
                } else {
                    JMRLog.trace(LOGGER, "Reduce task {} still running", assigned.taskId());
                }
            }

            assignedReduceTasks.removeAll(toRemove);

            JmrUtils.sleep(JMRConstants.REDUCE_TASK_MONITOR_SLEEP_MS);
        }

        int x = 10;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedReduceTasks(List<Worker> workers,
            final List<AssignedReduceTasks> assignedReduceTasks, final Queue<UnassignedReduceTasks> unassignedReduceQueue,
            final AtomicBoolean stopForError, JobConfiguration<D, V, O> jobConfig) {
        while (!(unassignedReduceQueue.isEmpty() && assignedReduceTasks.isEmpty()) && !stopForError.get()) {
            final UnassignedReduceTasks task = unassignedReduceQueue.poll();
            if (task == null) {
                continue;
            }
            JMRLog.debug(LOGGER, "Starting reduce task assignment process for {}", task.taskId());

            final Optional<AssignedReduceTasks> assigned = tryAssignReduceTask(task, workers, assignedReduceTasks, jobConfig);

            if (assigned.isPresent()) {
                JMRLog.debug(LOGGER, "Reduce task {} assigned successfully to {}", task.taskId(),
                        assigned.get().worker().getAddress() + ":" + assigned.get().worker().getPort());
                assignedReduceTasks.add(assigned.get());

            } else {
                JMRLog.debug(LOGGER, "No worker available for reduce task {}", task.taskId());
                unassignedReduceQueue.offer(task);

                JmrUtils.sleep(JMRConstants.REDUCE_TASK_SCHEDULER_SLEEP_MS);
            }

        }
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedReduceTasks> tryAssignReduceTask(
            UnassignedReduceTasks task, List<Worker> workers, List<AssignedReduceTasks> assignedReduceTasks, JobConfiguration<D, V, O> jobConfig) {
        final Set<Worker> busy = assignedReduceTasks.stream().map(AssignedReduceTasks::worker).collect(Collectors.toSet());
        for (final Worker worker : workers) {
            if (busy.contains(worker)) {
                continue;
            }

            final boolean assigned = worker.submitReduceTask(task.jobId(), task.taskId(), task.jarId(), task.partitionId(),
                    task.intermediateDataLocations(), jobConfig);
            if (assigned) {
                return Optional.of(task.toAssigned(worker));
            }
        }
        return Optional.empty();
    }

    private static void monitorAssignedMapTasks(final List<UnassignedMapTasks> mapTasks, final List<AssignedMapTasks> assignedTasks,
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
