package it.jmr.master;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
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

import io.grpc.StatusRuntimeException;
import it.jmr.master.events.WorkerFailureListener;

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

    /**
     * Encapsulates all the state for a job execution, allowing fault tolerance
     * handling.
     */
    public static class JobExecutionContext<O extends Serializable> {
        // MAP phase state
        public final Queue<UnassignedMapTasks> unassignedMapQueue;
        public final List<AssignedMapTasks> assignedMapTasks;
        public final List<CompletedMapTasks> completedMapTasks;
        public final List<UnassignedMapTasks> originalMapTasks;

        // REDUCE phase state
        public final Queue<UnassignedReduceTasks> unassignedReduceQueue;
        public final List<AssignedReduceTasks> assignedReduceTasks;
        public final List<CompletedReduceTasks<O>> completedReduceTasks;
        public final List<UnassignedReduceTasks> originalReduceTasks;

        // Phase tracking
        public volatile boolean inReducePhase;

        public JobExecutionContext() {
            this.unassignedMapQueue = new ConcurrentLinkedQueue<>();
            this.assignedMapTasks = new CopyOnWriteArrayList<>();
            this.completedMapTasks = new CopyOnWriteArrayList<>();
            this.originalMapTasks = new CopyOnWriteArrayList<>();

            this.unassignedReduceQueue = new ConcurrentLinkedQueue<>();
            this.assignedReduceTasks = new CopyOnWriteArrayList<>();
            this.completedReduceTasks = new CopyOnWriteArrayList<>();
            this.originalReduceTasks = new CopyOnWriteArrayList<>();

            this.inReducePhase = false;
        }

        public void initMapPhase(List<UnassignedMapTasks> mapTasks) {
            this.originalMapTasks.addAll(mapTasks);
            this.unassignedMapQueue.addAll(mapTasks);
        }

        public void initReducePhase(List<UnassignedReduceTasks> reduceTasks) {
            this.inReducePhase = true;
            this.originalReduceTasks.addAll(reduceTasks);
            this.unassignedReduceQueue.addAll(reduceTasks);
        }
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
                    executeJob(job, ctx.workers, ctx);
                } catch (JMRException e) {
                    JMRLog.error(LOGGER, "Error executing job {}: {}", job.getJobId(), e.getMessage());
                } catch (Exception e) {
                    JMRLog.error(LOGGER, "Unexpected error executing job {}: {}", job.getJobId(), e.getMessage());
                } catch (Throwable t) {
                    JMRLog.error(LOGGER, "Fatal error executing job {}: {} {}", job.getJobId(), t.getCause(), t.getMessage());
                }
            }

            JmrUtils.sleep(JMRConstants.MASTER_POLL_INTERVAL_MS);
        }
    }

    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> void executeJob(
            JobInfoInternal jobInfo, List<Worker> workers, MasterContext masterCtx) throws JMRException {

        LOGGER.info("\n>>> Starting job execution: {}", jobInfo.getJobId());
        jobInfo.setStatus(JobStatus.RUNNING);

        final String jarId = jobInfo.getJarId();
        final String jobId = jobInfo.getJobId();
        final Path jobSerializedPath = jobInfo.getJobPath();
        final Path jarPath = jobInfo.getJarPath();

        // Create job execution context for fault tolerance
        final JobExecutionContext<O> jobCtx = new JobExecutionContext<>();

        // Register worker failure listener
        final WorkerFailureListener failureListener = worker -> handleWorkerFailure(worker, jobCtx);
        if (masterCtx != null) {
            masterCtx.addWorkerFailureListener(failureListener);
        }

        try {
            // Deserialize job configuration
            final JobConfiguration<D, V, O> jobConfig = deserializeJobConfig(jobInfo, jarPath, jobSerializedPath);

            // Loading expected data size to proper partition
            final long dataSize = getDataSize(jobInfo, jobConfig);

            // Partition data into separate map tasks
            final List<UnassignedMapTasks> mapTasks = chunkIntoMapTasks(workers, jarId, jobId, dataSize);

            // Initialize MAP phase in context
            jobCtx.initMapPhase(mapTasks);

            final AtomicBoolean stopForError = new AtomicBoolean(false);

            final Future<?> assignerFuture = ExecutorManager.getExecutor()
                    .submit(() -> submitUnassignedMapTasks(workers, jobCtx.assignedMapTasks, jobCtx.unassignedMapQueue, stopForError, jobConfig));

            final Future<?> monitorFuture = ExecutorManager.getExecutor().submit(() -> monitorAssignedMapTasks(jobCtx.originalMapTasks,
                    jobCtx.assignedMapTasks, jobCtx.completedMapTasks, jobCtx.unassignedMapQueue, stopForError));

            executeMapping(jobInfo, stopForError, assignerFuture, monitorFuture);

            JMRLog.debug(LOGGER, "MAP PHASE COMPLETED");

            JMRLog.debug(LOGGER, "PARTITIONING AND SHUFFLING DATA");

            final Map<String, List<IntermediateLocation>> partitions = jobCtx.completedMapTasks.stream().map(c -> c.intermediateDataLocations())
                    .flatMap(List::stream).collect(Collectors.groupingBy(IntermediateLocation::getPartitionId));

            JMRLog.debug(LOGGER, "STARTING REDUCE PHASE");

            final List<UnassignedReduceTasks> reduceTasks = createReduceTasks(jobInfo, jarId, partitions).stream().toList();

            // Initialize REDUCE phase in context
            jobCtx.initReducePhase(reduceTasks);

            stopForError.set(false); // Reset for reduce phase

            final Future<?> reduceAssignerFuture = ExecutorManager.getExecutor().submit(
                    () -> submitUnassignedReduceTasks(workers, jobCtx.assignedReduceTasks, jobCtx.unassignedReduceQueue, stopForError, jobConfig));

            final Future<?> reduceMonitorFuture = ExecutorManager.getExecutor().submit(() -> monitorAssignedReduceTasks(jobCtx.originalReduceTasks,
                    jobCtx.assignedReduceTasks, jobCtx.completedReduceTasks, jobCtx.unassignedReduceQueue, stopForError));

            reduce(jobInfo, stopForError, reduceAssignerFuture, reduceMonitorFuture);

            JMRLog.debug(LOGGER, "REDUCE PHASE COMPLETED");

            // Collect final results (for now, just log them)
            for (final CompletedReduceTasks<O> task : jobCtx.completedReduceTasks) {
                JMRLog.info(LOGGER, "Reduced data for partition {}: {}", task.partitionId(), task.reducedData());
            }

            jobInfo.setStatus(JobStatus.COMPLETED);
            LOGGER.info("<<< Job completed: {} (time: {}ms)\n", jobInfo.getJobId(), jobInfo.getExecutionTime());
        } finally {
            // Unregister worker failure listener
            if (masterCtx != null) {
                masterCtx.removeWorkerFailureListener(failureListener);
            }
        }
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
            JobInfoInternal jobInfo, final Path jobJarPath, final Path jobSerializedPath) throws JMRException {

        final JobConfiguration<D, V, O> jobConfig;
        JobClassLoader loader = new JobClassLoader(jobJarPath, jobSerializedPath);
        try {
            jobConfig = loader.deserializeFromFile();
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

    /**
     * Handles worker failure by rescheduling tasks and invalidating lost
     * intermediate data.
     */
    static <O extends Serializable> void handleWorkerFailure(Worker failedWorker, JobExecutionContext<O> ctx) {
        JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Handling failure of worker {}", failedWorker.getWorkerId());

        // 1. Reschedule MAP tasks that were running on the failed worker
        final List<AssignedMapTasks> mapTasksToReschedule = ctx.assignedMapTasks.stream().filter(t -> t.worker().equals(failedWorker)).toList();

        for (AssignedMapTasks task : mapTasksToReschedule) {
            ctx.assignedMapTasks.remove(task);
            ctx.unassignedMapQueue.offer(task.toUnassigned());
            JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Rescheduled MAP task {} from failed worker {}", task.taskId(), failedWorker.getWorkerId());
        }

        if (!mapTasksToReschedule.isEmpty()) {
            JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Worker {} failed, rescheduling {} MAP tasks", failedWorker.getWorkerId(),
                    mapTasksToReschedule.size());
        }

        // 2. If in REDUCE phase, handle lost intermediate data
        if (ctx.inReducePhase) {
            // Reschedule REDUCE tasks running on failed worker
            final List<AssignedReduceTasks> reduceTasksToReschedule = ctx.assignedReduceTasks.stream().filter(t -> t.worker().equals(failedWorker))
                    .toList();

            for (AssignedReduceTasks task : reduceTasksToReschedule) {
                ctx.assignedReduceTasks.remove(task);
                ctx.unassignedReduceQueue.offer(task.toUnassigned());
                JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Rescheduled REDUCE task {} from failed worker {}", task.taskId(), failedWorker.getWorkerId());
            }

            // Find completed MAP tasks whose intermediate data is now lost
            final List<CompletedMapTasks> lostDataTasks = ctx.completedMapTasks.stream().filter(t -> t.worker().equals(failedWorker)).toList();

            if (!lostDataTasks.isEmpty()) {
                JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Lost intermediate data from {} completed MAP tasks", lostDataTasks.size());

                // Remove from completed and requeue for re-execution
                for (CompletedMapTasks task : lostDataTasks) {
                    ctx.completedMapTasks.remove(task);
                    ctx.unassignedMapQueue.offer(new UnassignedMapTasks(task.jobId(), task.taskId(), task.chunks(), task.jarId()));
                    JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Re-queued MAP task {} for re-execution (lost intermediate data)", task.taskId());
                }

                // Invalidate REDUCE tasks that depend on the lost data
                // Find completed REDUCE tasks that used data from failed worker
                final List<CompletedReduceTasks<O>> invalidReduceTasks = ctx.completedReduceTasks.stream()
                        .filter(t -> t.intermediateDataLocations().stream().anyMatch(loc -> loc.getWorkerId().equals(failedWorker.getWorkerId())))
                        .toList();

                for (CompletedReduceTasks<O> task : invalidReduceTasks) {
                    ctx.completedReduceTasks.remove(task);
                    // Don't requeue yet - will be recreated when MAP tasks complete again
                    JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Invalidated REDUCE task {} (depended on lost data)", task.taskId());
                }

                // Also invalidate assigned REDUCE tasks that depend on lost data
                final List<AssignedReduceTasks> invalidAssignedReduceTasks = ctx.assignedReduceTasks.stream()
                        .filter(t -> t.intermediateDataLocations().stream().anyMatch(loc -> loc.getWorkerId().equals(failedWorker.getWorkerId())))
                        .toList();

                for (AssignedReduceTasks task : invalidAssignedReduceTasks) {
                    ctx.assignedReduceTasks.remove(task);
                    // Don't requeue yet - will be recreated when MAP tasks complete again
                    JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Invalidated assigned REDUCE task {} (depended on lost data)", task.taskId());
                }

                // Also invalidate unassigned REDUCE tasks that depend on lost data
                final List<UnassignedReduceTasks> invalidUnassignedReduceTasks = new ArrayList<>();
                for (UnassignedReduceTasks task : ctx.unassignedReduceQueue) {
                    if (task.intermediateDataLocations().stream().anyMatch(loc -> loc.getWorkerId().equals(failedWorker.getWorkerId()))) {
                        invalidUnassignedReduceTasks.add(task);
                    }
                }

                for (UnassignedReduceTasks task : invalidUnassignedReduceTasks) {
                    ctx.unassignedReduceQueue.remove(task);
                    JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Removed unassigned REDUCE task {} (depended on lost data)", task.taskId());
                }

                int totalInvalidated = invalidReduceTasks.size() + invalidAssignedReduceTasks.size() + invalidUnassignedReduceTasks.size();
                if (totalInvalidated > 0) {
                    JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Invalidated {} REDUCE tasks pending MAP re-execution", totalInvalidated);
                }
            }
        }
    }

    private static <O extends Serializable> void monitorAssignedReduceTasks(final List<UnassignedReduceTasks> reduceTasks,
            final List<AssignedReduceTasks> assignedReduceTasks, final List<CompletedReduceTasks<O>> completedReduceTasks,
            final Queue<UnassignedReduceTasks> unassignedReduceQueue, final AtomicBoolean stopForError) {

        final int totalTasks = reduceTasks.size();
        int lastLoggedPercentage = -1;

        while (!stopForError.get() && completedReduceTasks.size() < totalTasks) {
            // Calcolo della percentuale corrente
            int currentCompleted = completedReduceTasks.size();
            int currentPercentage = (totalTasks > 0) ? (currentCompleted * 100 / totalTasks) : 100;

            if (currentPercentage >= lastLoggedPercentage + 2) {
                JMRLog.info(LOGGER, "Reduce Progress: {}% ({} su {} task completati)", currentPercentage, currentCompleted, totalTasks);
                lastLoggedPercentage = currentPercentage;
            }

            JMRLog.debug(LOGGER, "Monitoring assigned reduce tasks: {} completed out of {}", currentCompleted, totalTasks);
            final List<AssignedReduceTasks> toRemove = new ArrayList<>();

            for (final AssignedReduceTasks assigned : assignedReduceTasks) {
                try {
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
                } catch (StatusRuntimeException e) {
                    // gRPC error indicates worker is likely dead - reschedule immediately
                    JMRLog.warn(LOGGER, "[FAULT-TOLERANCE] gRPC error for reduce task {} on worker {}, re-queuing: {}", assigned.taskId(),
                            assigned.worker().getWorkerId(), e.getStatus());
                    unassignedReduceQueue.offer(assigned.toUnassigned());
                    toRemove.add(assigned);
                }
            }

            assignedReduceTasks.removeAll(toRemove);
            JmrUtils.sleep(JMRConstants.REDUCE_TASK_MONITOR_SLEEP_MS);
        }

        // Log finale per confermare il 100%
        if (!stopForError.get()) {
            JMRLog.info(LOGGER, "Reduce Progress: 100% ({} su {} task completati)", totalTasks, totalTasks);
        }
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedReduceTasks(List<Worker> workers,
            final List<AssignedReduceTasks> assignedReduceTasks, final Queue<UnassignedReduceTasks> unassignedReduceQueue,
            final AtomicBoolean stopForError, JobConfiguration<D, V, O> jobConfig) {
        while (!(unassignedReduceQueue.isEmpty() && assignedReduceTasks.isEmpty()) && !stopForError.get()) {
            final UnassignedReduceTasks task = unassignedReduceQueue.poll();
            if (task == null) {
                JmrUtils.sleep(JMRConstants.REDUCE_TASK_SCHEDULER_SLEEP_MS);
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
                // JmrUtils.sleep(JMRConstants.REDUCE_TASK_SCHEDULER_SLEEP_MS);

            }

        }
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedReduceTasks> tryAssignReduceTask(
            UnassignedReduceTasks task, List<Worker> workers, List<AssignedReduceTasks> assignedReduceTasks, JobConfiguration<D, V, O> jobConfig) {
        final Set<Worker> busy = assignedReduceTasks.stream().map(AssignedReduceTasks::worker).collect(Collectors.toSet());
        for (final Worker worker : workers) {
            if (busy.contains(worker)) {
                JMRLog.debug(LOGGER, "Worker {} is busy, skipping for reduce task {}", worker.getWorkerId(), task.taskId());
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
                try {
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
                } catch (StatusRuntimeException e) {
                    // gRPC error indicates worker is likely dead - reschedule immediately
                    JMRLog.warn(LOGGER, "[FAULT-TOLERANCE] gRPC error for task {} on worker {}, re-queuing: {}", assigned.taskId(),
                            assigned.worker().getWorkerId(), e.getStatus());
                    unassignedQueue.offer(assigned.toUnassigned());
                    toRemove.add(assigned);
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

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedMapTasks(List<Worker> workers,
            final List<AssignedMapTasks> assignedTasks, final Queue<UnassignedMapTasks> unassignedQueue, final AtomicBoolean stopForError,
            JobConfiguration<D, V, O> jobConfig) {
        while (!(unassignedQueue.isEmpty() && assignedTasks.isEmpty()) && !stopForError.get()) {
            final UnassignedMapTasks task = unassignedQueue.poll();
            if (task == null) {
                JmrUtils.sleep(JMRConstants.MAP_TASK_SCHEDULER_SLEEP_MS);
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
                JmrUtils.sleep(JMRConstants.MAP_TASK_SCHEDULER_SLEEP_MS);

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
                JMRLog.debug(LOGGER, "Worker {} is busy, skipping for task {}", worker.getWorkerId(), task.taskId());
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
