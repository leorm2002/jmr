package it.jmr.master;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.StatusRuntimeException;
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
import it.jmr.master.events.WorkerFailureListener;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

public class MasterExecutor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterExecutor.class);

    private enum PhaseOutcome {
        COMPLETED,
        CANCELLED,
        REPLAY_REQUIRED
    }

    public record UnassignedMapTasks(String jobId, String taskId, Pair<Long, Long> chunks, String jarId) {
        public AssignedMapTasks toAssigned(final Worker worker) {
            return new AssignedMapTasks(worker, jobId, taskId, chunks, jarId);
        }
    }

    public record AssignedMapTasks(Worker worker, String jobId, String taskId, Pair<Long, Long> chunks, String jarId) {
        CompletedMapTasks toCompleted(final List<IntermediateLocation> intermediateDataLocations) {
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
        public AssignedReduceTasks toAssigned(final Worker worker) {
            return new AssignedReduceTasks(worker, jobId, taskId, jarId, partitionId, intermediateDataLocations);
        }
    }

    public record AssignedReduceTasks(Worker worker, String jobId, String taskId, String jarId, String partitionId,
            List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations) {
        <O extends Serializable> CompletedReduceTasks<O> toCompleted(final List<Pair<String, O>> reducedData) {
            return new CompletedReduceTasks<>(worker, jobId, taskId, jarId, partitionId, intermediateDataLocations, reducedData);
        }

        UnassignedReduceTasks toUnassigned() {
            return new UnassignedReduceTasks(jobId, taskId, jarId, partitionId, intermediateDataLocations);
        }
    }

    public record CompletedReduceTasks<O extends Serializable>(Worker worker, String jobId, String taskId, String jarId, String partitionId,
            List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations, List<Pair<String, O>> reducedData) {
    }

    public static class JobExecutionContext<O extends Serializable> {
        public final Queue<UnassignedMapTasks> unassignedMapQueue;
        public final Set<String> queuedMapTaskIds;
        public final List<AssignedMapTasks> assignedMapTasks;
        public final List<CompletedMapTasks> completedMapTasks;
        public final List<UnassignedMapTasks> originalMapTasks;
        public final Map<String, Integer> mapAttempts;

        public final Queue<UnassignedReduceTasks> unassignedReduceQueue;
        public final Set<String> queuedReduceTaskIds;
        public final List<AssignedReduceTasks> assignedReduceTasks;
        public final List<CompletedReduceTasks<O>> completedReduceTasks;
        public final List<UnassignedReduceTasks> originalReduceTasks;
        public final Map<String, Integer> reduceAttempts;

        public volatile boolean inReducePhase;
        public final AtomicBoolean mapReplayRequested;
        private volatile Consumer<String> eventSink;

        public JobExecutionContext() {
            this.unassignedMapQueue = new ConcurrentLinkedQueue<>();
            this.queuedMapTaskIds = ConcurrentHashMap.newKeySet();
            this.assignedMapTasks = new CopyOnWriteArrayList<>();
            this.completedMapTasks = new CopyOnWriteArrayList<>();
            this.originalMapTasks = new CopyOnWriteArrayList<>();
            this.mapAttempts = new ConcurrentHashMap<>();

            this.unassignedReduceQueue = new ConcurrentLinkedQueue<>();
            this.queuedReduceTaskIds = ConcurrentHashMap.newKeySet();
            this.assignedReduceTasks = new CopyOnWriteArrayList<>();
            this.completedReduceTasks = new CopyOnWriteArrayList<>();
            this.originalReduceTasks = new CopyOnWriteArrayList<>();
            this.reduceAttempts = new ConcurrentHashMap<>();

            this.inReducePhase = false;
            this.mapReplayRequested = new AtomicBoolean(false);
            this.eventSink = ignored -> {
            };
        }

        public void setEventSink(final Consumer<String> eventSink) {
            this.eventSink = eventSink == null ? ignored -> {
            } : eventSink;
        }

        public void recordEvent(final String message) {
            eventSink.accept(message);
        }

        public void initMapPhase(final List<UnassignedMapTasks> mapTasks) {
            if (originalMapTasks.isEmpty()) {
                originalMapTasks.addAll(mapTasks);
            }
            enqueueMapTasks(mapTasks);
        }

        public void initReducePhase(final List<UnassignedReduceTasks> reduceTasks) {
            resetReducePhaseState();
            inReducePhase = true;
            originalReduceTasks.addAll(reduceTasks);
            enqueueReduceTasks(reduceTasks);
        }

        public void resetReducePhaseState() {
            inReducePhase = false;
            mapReplayRequested.set(false);
            unassignedReduceQueue.clear();
            queuedReduceTaskIds.clear();
            assignedReduceTasks.clear();
            completedReduceTasks.clear();
            originalReduceTasks.clear();
            reduceAttempts.clear();
        }

        public void enqueueMapTask(final UnassignedMapTasks task) {
            if (queuedMapTaskIds.add(task.taskId())) {
                unassignedMapQueue.offer(task);
            }
        }

        public void enqueueMapTasks(final List<UnassignedMapTasks> tasks) {
            tasks.forEach(this::enqueueMapTask);
        }

        public UnassignedMapTasks pollMapTask() {
            final UnassignedMapTasks task = unassignedMapQueue.poll();
            if (task != null) {
                queuedMapTaskIds.remove(task.taskId());
            }
            return task;
        }

        public void enqueueReduceTask(final UnassignedReduceTasks task) {
            if (queuedReduceTaskIds.add(task.taskId())) {
                unassignedReduceQueue.offer(task);
            }
        }

        public void enqueueReduceTasks(final List<UnassignedReduceTasks> tasks) {
            tasks.forEach(this::enqueueReduceTask);
        }

        public UnassignedReduceTasks pollReduceTask() {
            final UnassignedReduceTasks task = unassignedReduceQueue.poll();
            if (task != null) {
                queuedReduceTaskIds.remove(task.taskId());
            }
            return task;
        }

        public void removeQueuedReduceTask(final UnassignedReduceTasks task) {
            if (queuedReduceTaskIds.remove(task.taskId())) {
                unassignedReduceQueue.remove(task);
            }
        }
    }

    private final MasterContext ctx;

    public MasterExecutor(final MasterContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
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
            final JobInfoInternal jobInfo, final List<Worker> workers, final MasterContext masterCtx) throws JMRException {
        if (jobInfo.isCancellationRequested() || jobInfo.getStatus() == JobStatus.CANCELLED) {
            markJobCancelled(jobInfo);
            return;
        }

        LOGGER.info("\n>>> Starting job execution: {}", jobInfo.getJobId());
        jobInfo.clearSerializedResult();
        jobInfo.setErrorMessage("");
        jobInfo.setMapProgress(0);
        jobInfo.setReduceProgress(0);
        jobInfo.setStatus(JobStatus.RUNNING);

        final String jarId = jobInfo.getJarId();
        final String jobId = jobInfo.getJobId();
        final Path jobSerializedPath = jobInfo.getJobPath();
        final Path jarPath = jobInfo.getJarPath();
        final JobExecutionContext<O> jobCtx = new JobExecutionContext<>();
        final WorkerFailureListener failureListener = worker -> handleWorkerFailure(worker, jobCtx);

        if (masterCtx != null) {
            masterCtx.addWorkerFailureListener(failureListener);
            masterCtx.setActiveExecution(jobInfo, jobCtx, "PREPARING");
            masterCtx.recordEvent("Job " + jobInfo.getJobId() + " started");
            jobCtx.setEventSink(masterCtx::recordEvent);
        }

        try {
            final JobConfiguration<D, V, O> jobConfig = deserializeJobConfig(jobInfo, jarPath, jobSerializedPath);
            if (jobInfo.isCancellationRequested()) {
                markJobCancelled(jobInfo);
                return;
            }

            final long dataSize = getDataSize(jobInfo, jobConfig);
            ensureWorkersAvailable(jobInfo, workers);
            final int reducePartitionCount = Math.max(1, workers.size() * JMRConstants.REDUCE_BUCKETS_PER_WORKER);

            final List<UnassignedMapTasks> mapTasks = chunkIntoMapTasks(workers, jarId, jobId, dataSize);
            jobCtx.initMapPhase(mapTasks);
            if (masterCtx != null) {
                masterCtx.updateActivePhase("MAP");
            }

            final AtomicBoolean stopForError = new AtomicBoolean(false);
            final Future<?> assignerFuture = ExecutorManager.getExecutor()
                    .submit(() -> submitUnassignedMapTasks(jobInfo, workers, jobCtx, stopForError, reducePartitionCount, jobConfig));
            final Future<?> monitorFuture = ExecutorManager.getExecutor()
                    .submit(() -> monitorAssignedMapTasks(jobInfo, jobCtx, stopForError));

            if (executeMapping(jobInfo, stopForError, assignerFuture, monitorFuture) == PhaseOutcome.CANCELLED) {
                return;
            }

            JMRLog.debug(LOGGER, "MAP PHASE COMPLETED");
            jobInfo.setMapProgress(100);
            jobInfo.setReduceProgress(0);

            while (true) {
                if (jobInfo.isCancellationRequested()) {
                    markJobCancelled(jobInfo);
                    return;
                }

                final Map<String, List<IntermediateLocation>> partitions = jobCtx.completedMapTasks.stream()
                        .map(CompletedMapTasks::intermediateDataLocations).flatMap(List::stream)
                        .collect(Collectors.groupingBy(IntermediateLocation::getPartitionId));

                final List<UnassignedReduceTasks> reduceTasks = createReduceTasks(jobInfo, jarId, partitions);
                jobCtx.initReducePhase(reduceTasks);
                if (masterCtx != null) {
                    masterCtx.updateActivePhase("REDUCE");
                }
                stopForError.set(false);

                final Future<?> reduceAssignerFuture = ExecutorManager.getExecutor()
                        .submit(() -> submitUnassignedReduceTasks(jobInfo, workers, jobCtx, stopForError, jobConfig));
                final Future<?> reduceMonitorFuture = ExecutorManager.getExecutor()
                        .submit(() -> monitorAssignedReduceTasks(jobInfo, jobCtx, stopForError));

                final PhaseOutcome reduceOutcome = reduce(jobInfo, stopForError, jobCtx.mapReplayRequested, reduceAssignerFuture,
                        reduceMonitorFuture);
                if (reduceOutcome == PhaseOutcome.CANCELLED) {
                    return;
                }
                if (reduceOutcome == PhaseOutcome.COMPLETED) {
                    break;
                }

                JMRLog.warn(LOGGER,
                        "[FAULT-TOLERANCE] Reduce phase invalidated by lost intermediate data. Re-running pending MAP tasks before retrying REDUCE.");
                jobCtx.resetReducePhaseState();
                jobInfo.setReduceProgress(0);
                if (executePendingMapTasks(jobInfo, workers, jobCtx, stopForError, reducePartitionCount, jobConfig) == PhaseOutcome.CANCELLED) {
                    return;
                }
            }

            jobInfo.setSerializedResult(serializeJobResult(jobCtx.completedReduceTasks));
            jobInfo.setStatus(JobStatus.COMPLETED);
            jobCtx.recordEvent("Job " + jobInfo.getJobId() + " completed");
            LOGGER.info("<<< Job completed: {} (time: {}ms)\n", jobInfo.getJobId(), jobInfo.getExecutionTime());
        } finally {
            if (masterCtx != null) {
                masterCtx.removeWorkerFailureListener(failureListener);
                masterCtx.clearActiveExecution();
            }
        }
    }

    private static PhaseOutcome reduce(final JobInfoInternal jobInfo, final AtomicBoolean stopForError, final AtomicBoolean mapReplayRequested,
            final Future<?> reduceAssignerFuture, final Future<?> reduceMonitorFuture) throws JMRException {
        try {
            reduceAssignerFuture.get();
            reduceMonitorFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (jobInfo.isCancellationRequested()) {
                return cancelExecution(jobInfo);
            }
            stopForError.set(true);
            failJob(jobInfo, "Reduce phase interrupted");
            throw new JMRException("Reduce phase interrupted", e);
        } catch (Exception e) {
            if (jobInfo.isCancellationRequested()) {
                return cancelExecution(jobInfo);
            }
            stopForError.set(true);
            failJob(jobInfo, "Error during reduce phase: " + e.getMessage());
            throw new JMRException("Error during reduce phase", e);
        }

        if (jobInfo.isCancellationRequested()) {
            return cancelExecution(jobInfo);
        }
        if (stopForError.get() || jobInfo.getStatus() == JobStatus.FAILED) {
            throw new JMRException("Reduce phase failed: " + jobInfo.getErrorMessage());
        }
        if (mapReplayRequested.get()) {
            return PhaseOutcome.REPLAY_REQUIRED;
        }
        return PhaseOutcome.COMPLETED;
    }

    private static List<UnassignedReduceTasks> createReduceTasks(final JobInfoInternal jobInfo, final String jarId,
            final Map<String, List<IntermediateLocation>> partitions) {
        final List<UnassignedReduceTasks> reduceTasks = new ArrayList<>();
        for (Map.Entry<String, List<IntermediateLocation>> partition : partitions.entrySet()) {
            final String partitionId = partition.getKey();
            final List<it.jmr.grpc.worker.IntermediateDataLocation> locations = partition.getValue().stream()
                    .map(loc -> it.jmr.grpc.worker.IntermediateDataLocation.newBuilder().setWorkerId(loc.getWorkerId()).setTaskId(loc.getTaskId())
                            .setPartitionId(loc.getPartitionId()).setHost(loc.getHost()).setPort(loc.getPort()).build())
                    .collect(Collectors.toList());
            reduceTasks.add(new UnassignedReduceTasks(jobInfo.getJobId(), jobInfo.getJobId() + "-reduce-" + partitionId, jarId, partitionId,
                    locations));
        }
        return reduceTasks;
    }

    private static PhaseOutcome executeMapping(final JobInfoInternal jobInfo, final AtomicBoolean stopForError, final Future<?> assignerFuture,
            final Future<?> monitorFuture) throws JMRException {
        try {
            assignerFuture.get();
            monitorFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (jobInfo.isCancellationRequested()) {
                return cancelExecution(jobInfo);
            }
            stopForError.set(true);
            failJob(jobInfo, "Map phase interrupted");
            throw new JMRException("Map phase interrupted", e);
        } catch (Exception e) {
            if (jobInfo.isCancellationRequested()) {
                return cancelExecution(jobInfo);
            }
            stopForError.set(true);
            failJob(jobInfo, "Error during map phase: " + e.getMessage());
            throw new JMRException("Error during map phase", e);
        }

        if (jobInfo.isCancellationRequested()) {
            return cancelExecution(jobInfo);
        }
        if (stopForError.get() || jobInfo.getStatus() == JobStatus.FAILED) {
            throw new JMRException("Map phase failed: " + jobInfo.getErrorMessage());
        }
        return PhaseOutcome.COMPLETED;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> PhaseOutcome executePendingMapTasks(
            final JobInfoInternal jobInfo, final List<Worker> workers, final JobExecutionContext<O> jobCtx, final AtomicBoolean stopForError,
            final int reducePartitionCount, final JobConfiguration<D, V, O> jobConfig) throws JMRException {
        jobCtx.mapReplayRequested.set(false);
        stopForError.set(false);

        final Future<?> replayMapAssignerFuture = ExecutorManager.getExecutor()
                .submit(() -> submitUnassignedMapTasks(jobInfo, workers, jobCtx, stopForError, reducePartitionCount, jobConfig));
        final Future<?> replayMapMonitorFuture = ExecutorManager.getExecutor()
                .submit(() -> monitorAssignedMapTasks(jobInfo, jobCtx, stopForError));

        return executeMapping(jobInfo, stopForError, replayMapAssignerFuture, replayMapMonitorFuture);
    }

    private static void ensureWorkersAvailable(final JobInfoInternal jobInfo, final List<Worker> workers) throws JMRException {
        if (!workers.isEmpty()) {
            return;
        }

        failJob(jobInfo, "No workers available to execute the job");
        throw new JMRException("No workers available to execute the job");
    }

    private static List<UnassignedMapTasks> chunkIntoMapTasks(final List<Worker> workers, final String jarId, final String jobId,
            final long dataSize) {
        final List<UnassignedMapTasks> mapTasks = new ArrayList<>();
        if (workers.isEmpty()) {
            throw new IllegalArgumentException("At least one worker is required to create map tasks");
        }
        if (dataSize <= 0) {
            return mapTasks;
        }

        final int n = Math.max(1, workers.size() * 3);
        final long chunkSize = Math.max(1L, Math.ceilDiv(dataSize, n));
        final List<Pair<Long, Long>> chunks = buildChunks(dataSize, chunkSize);
        JMRLog.info(LOGGER, "Job {}: data size = {}, divided in {} chunks", jobId, dataSize, chunks.size());

        for (int i = 0; i < chunks.size(); i++) {
            mapTasks.add(new UnassignedMapTasks(jobId, jobId + "-map-" + i, chunks.get(i), jarId));
        }
        return mapTasks;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> long getDataSize(final JobInfoInternal jobInfo,
            final JobConfiguration<D, V, O> jobConfig) throws JMRException {
        final long dataSize;
        try (final DataProviderClient<D> provider = jobConfig.getDataProvider()) {
            provider.init();
            dataSize = provider.size();
        } catch (Exception e) {
            failJob(jobInfo, "Error loading job configuration: " + e.getMessage());
            throw new JMRException("Error loading job configuration: " + e.getMessage(), e);
        }
        return dataSize;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> JobConfiguration<D, V, O> deserializeJobConfig(
            final JobInfoInternal jobInfo, final Path jobJarPath, final Path jobSerializedPath) throws JMRException {
        try {
            return new JobClassLoader(jobJarPath, jobSerializedPath).deserializeFromFile();
        } catch (Exception e) {
            failJob(jobInfo, "Error loading job configuration: " + e.getMessage());
            JMRLog.error(LOGGER, "Full deserialization error:", e);
            throw new JMRException("Error loading job configuration: " + e.getMessage(), e);
        }
    }

    static <O extends Serializable> void handleWorkerFailure(final Worker failedWorker, final JobExecutionContext<O> ctx) {
        JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Handling failure of worker {}", failedWorker.getWorkerId());

        final List<AssignedMapTasks> mapTasksToReschedule = ctx.assignedMapTasks.stream().filter(t -> t.worker().equals(failedWorker)).toList();
        for (AssignedMapTasks task : mapTasksToReschedule) {
            ctx.assignedMapTasks.remove(task);
            ctx.enqueueMapTask(task.toUnassigned());
            JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Rescheduled MAP task {} from failed worker {}", task.taskId(), failedWorker.getWorkerId());
        }

        if (!ctx.inReducePhase) {
            return;
        }

        final List<CompletedMapTasks> lostDataTasks = ctx.completedMapTasks.stream().filter(t -> t.worker().equals(failedWorker)).toList();
        if (!lostDataTasks.isEmpty()) {
            JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Lost intermediate data from {} completed MAP tasks", lostDataTasks.size());

            for (CompletedMapTasks task : lostDataTasks) {
                ctx.completedMapTasks.remove(task);
                ctx.enqueueMapTask(new UnassignedMapTasks(task.jobId(), task.taskId(), task.chunks(), task.jarId()));
                JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Re-queued MAP task {} for re-execution (lost intermediate data)", task.taskId());
            }

            final List<CompletedReduceTasks<O>> invalidCompletedReduceTasks = ctx.completedReduceTasks.stream()
                    .filter(task -> dependsOnWorker(task.intermediateDataLocations(), failedWorker.getWorkerId())).toList();
            for (CompletedReduceTasks<O> task : invalidCompletedReduceTasks) {
                ctx.completedReduceTasks.remove(task);
            }

            final List<AssignedReduceTasks> invalidAssignedReduceTasks = ctx.assignedReduceTasks.stream()
                    .filter(task -> task.worker().equals(failedWorker)
                            || dependsOnWorker(task.intermediateDataLocations(), failedWorker.getWorkerId()))
                    .toList();
            for (AssignedReduceTasks task : invalidAssignedReduceTasks) {
                ctx.assignedReduceTasks.remove(task);
            }

            final List<UnassignedReduceTasks> invalidQueuedReduceTasks = ctx.unassignedReduceQueue.stream()
                    .filter(task -> dependsOnWorker(task.intermediateDataLocations(), failedWorker.getWorkerId())).toList();
            for (UnassignedReduceTasks task : invalidQueuedReduceTasks) {
                ctx.removeQueuedReduceTask(task);
            }

            final int totalInvalidated = invalidCompletedReduceTasks.size() + invalidAssignedReduceTasks.size() + invalidQueuedReduceTasks.size();
            if (totalInvalidated > 0) {
                JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Invalidated {} REDUCE tasks pending MAP re-execution", totalInvalidated);
            }
            ctx.mapReplayRequested.set(true);
            return;
        }

        final List<AssignedReduceTasks> reduceTasksToReschedule = ctx.assignedReduceTasks.stream().filter(t -> t.worker().equals(failedWorker))
                .toList();
        for (AssignedReduceTasks task : reduceTasksToReschedule) {
            ctx.assignedReduceTasks.remove(task);
            ctx.enqueueReduceTask(task.toUnassigned());
            JMRLog.info(LOGGER, "[FAULT-TOLERANCE] Rescheduled REDUCE task {} from failed worker {}", task.taskId(), failedWorker.getWorkerId());
        }
    }

    private static boolean dependsOnWorker(final List<it.jmr.grpc.worker.IntermediateDataLocation> intermediateDataLocations,
            final String workerId) {
        return intermediateDataLocations.stream().anyMatch(location -> location.getWorkerId().equals(workerId));
    }

    private static <O extends Serializable> void monitorAssignedReduceTasks(final JobInfoInternal jobInfo, final JobExecutionContext<O> jobCtx,
            final AtomicBoolean stopForError) {
        final int totalTasks = jobCtx.originalReduceTasks.size();
        int lastLoggedPercentage = -1;

        while (!stopForError.get() && !jobCtx.mapReplayRequested.get() && !jobInfo.isCancellationRequested()
                && jobCtx.completedReduceTasks.size() < totalTasks) {
            final int currentCompleted = jobCtx.completedReduceTasks.size();
            final int currentPercentage = totalTasks > 0 ? (currentCompleted * 100 / totalTasks) : 100;
            jobInfo.setReduceProgress(currentPercentage);

            if (currentPercentage >= lastLoggedPercentage + 2) {
                JMRLog.info(LOGGER, "Reduce Progress: {}% ({} su {} task completati)", currentPercentage, currentCompleted, totalTasks);
                lastLoggedPercentage = currentPercentage;
            }

            final List<AssignedReduceTasks> toRemove = new ArrayList<>();
            for (AssignedReduceTasks assigned : jobCtx.assignedReduceTasks) {
                try {
                    final Pair<WorkerTaskStatus, List<Pair<String, O>>> statusResponse = assigned.worker().getReduceTaskStatus(assigned.jobId(),
                            assigned.taskId());
                    final WorkerTaskStatus status = statusResponse.getFirst();

                    if (status == WorkerTaskStatus.COMPLETED) {
                        addCompletedReduceTask(jobCtx, assigned.toCompleted(statusResponse.getSecond()));
                        jobCtx.reduceAttempts.remove(assigned.taskId());
                        toRemove.add(assigned);
                    } else if (status == WorkerTaskStatus.FAILED || status == WorkerTaskStatus.MISSING) {
                        toRemove.add(assigned);
                        requeueReduceTask(jobInfo, jobCtx, assigned.toUnassigned(), stopForError, "state=" + status, true);
                    }
                } catch (StatusRuntimeException e) {
                    toRemove.add(assigned);
                    requeueReduceTask(jobInfo, jobCtx, assigned.toUnassigned(), stopForError, "gRPC error: " + e.getStatus(), false);
                }
            }

            jobCtx.assignedReduceTasks.removeAll(toRemove);
            JmrUtils.sleep(JMRConstants.REDUCE_TASK_MONITOR_SLEEP_MS);
        }

        if (!stopForError.get() && !jobCtx.mapReplayRequested.get() && !jobInfo.isCancellationRequested()) {
            jobInfo.setReduceProgress(100);
            JMRLog.info(LOGGER, "Reduce Progress: 100% ({} su {} task completati)", totalTasks, totalTasks);
        }
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedReduceTasks(
            final JobInfoInternal jobInfo, final List<Worker> workers, final JobExecutionContext<O> jobCtx, final AtomicBoolean stopForError,
            final JobConfiguration<D, V, O> jobConfig) {
        while (!(jobCtx.unassignedReduceQueue.isEmpty() && jobCtx.assignedReduceTasks.isEmpty()) && !stopForError.get()
                && !jobCtx.mapReplayRequested.get() && !jobInfo.isCancellationRequested()) {
            if (workers.isEmpty()) {
                throw new IllegalStateException("No workers available to execute reduce tasks");
            }

            final UnassignedReduceTasks task = jobCtx.pollReduceTask();
            if (task == null) {
                JmrUtils.sleep(JMRConstants.REDUCE_TASK_SCHEDULER_SLEEP_MS);
                continue;
            }

            final Optional<AssignedReduceTasks> assigned = tryAssignReduceTask(task, workers, jobCtx.assignedReduceTasks, jobConfig);
            if (assigned.isPresent()) {
                jobCtx.assignedReduceTasks.add(assigned.get());
                jobCtx.recordEvent("REDUCE assigned " + task.taskId() + " to " + assigned.get().worker().getWorkerId());
            } else {
                jobCtx.enqueueReduceTask(task);
                JmrUtils.sleep(JMRConstants.REDUCE_TASK_SCHEDULER_SLEEP_MS);
            }
        }
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedReduceTasks> tryAssignReduceTask(
            final UnassignedReduceTasks task, final List<Worker> workers, final List<AssignedReduceTasks> assignedReduceTasks,
            final JobConfiguration<D, V, O> jobConfig) {
        final Set<Worker> busy = assignedReduceTasks.stream().map(AssignedReduceTasks::worker).collect(Collectors.toSet());
        for (Worker worker : workers) {
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

    private static void monitorAssignedMapTasks(final JobInfoInternal jobInfo, final JobExecutionContext<?> jobCtx,
            final AtomicBoolean stopForError) {
        final int totalTasks = jobCtx.originalMapTasks.size();
        int lastLoggedPercentage = -1;

        while (!stopForError.get() && !jobInfo.isCancellationRequested() && jobCtx.completedMapTasks.size() < jobCtx.originalMapTasks.size()) {
            final int currentCompleted = jobCtx.completedMapTasks.size();
            final int currentPercentage = totalTasks > 0 ? (currentCompleted * 100 / totalTasks) : 100;
            jobInfo.setMapProgress(currentPercentage);

            if (currentPercentage >= lastLoggedPercentage + 2) {
                JMRLog.info(LOGGER, "Map Progress: {}% ({} su {} task completati)", currentPercentage, currentCompleted, totalTasks);
                lastLoggedPercentage = currentPercentage;
            }

            final List<AssignedMapTasks> toRemove = new ArrayList<>();
            for (AssignedMapTasks assigned : jobCtx.assignedMapTasks) {
                try {
                    final Pair<WorkerTaskStatus, List<IntermediateLocation>> statusResponse = assigned.worker().getMapTaskStatus(assigned.jobId(),
                            assigned.taskId());
                    final WorkerTaskStatus status = statusResponse.getFirst();

                    if (status == WorkerTaskStatus.COMPLETED) {
                        addCompletedMapTask(jobCtx, assigned.toCompleted(statusResponse.getSecond()));
                        jobCtx.mapAttempts.remove(assigned.taskId());
                        toRemove.add(assigned);
                    } else if (status == WorkerTaskStatus.FAILED || status == WorkerTaskStatus.MISSING) {
                        toRemove.add(assigned);
                        requeueMapTask(jobInfo, jobCtx, assigned.toUnassigned(), stopForError, "state=" + status, true);
                    }
                } catch (StatusRuntimeException e) {
                    toRemove.add(assigned);
                    requeueMapTask(jobInfo, jobCtx, assigned.toUnassigned(), stopForError, "gRPC error: " + e.getStatus(), false);
                }
            }

            jobCtx.assignedMapTasks.removeAll(toRemove);
            JmrUtils.sleep(JMRConstants.MAP_TASK_MONITOR_SLEEP_MS);
        }

        if (!stopForError.get() && !jobInfo.isCancellationRequested()) {
            jobInfo.setMapProgress(100);
            JMRLog.info(LOGGER, "Map Progress: 100% ({} su {} task completati)", totalTasks, totalTasks);
        }
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> void submitUnassignedMapTasks(
            final JobInfoInternal jobInfo, final List<Worker> workers, final JobExecutionContext<?> jobCtx, final AtomicBoolean stopForError,
            final int reducePartitionCount, final JobConfiguration<D, V, O> jobConfig) {
        while (!(jobCtx.unassignedMapQueue.isEmpty() && jobCtx.assignedMapTasks.isEmpty()) && !stopForError.get()
                && !jobInfo.isCancellationRequested()) {
            if (workers.isEmpty()) {
                throw new IllegalStateException("No workers available to execute map tasks");
            }

            final UnassignedMapTasks task = jobCtx.pollMapTask();
            if (task == null) {
                JmrUtils.sleep(JMRConstants.MAP_TASK_SCHEDULER_SLEEP_MS);
                continue;
            }

            final Optional<AssignedMapTasks> assigned = tryAssignMapTask(task, workers, jobCtx.assignedMapTasks, reducePartitionCount, jobConfig);
            if (assigned.isPresent()) {
                jobCtx.assignedMapTasks.add(assigned.get());
                jobCtx.recordEvent("MAP assigned " + task.taskId() + " to " + assigned.get().worker().getWorkerId());
            } else {
                jobCtx.enqueueMapTask(task);
                JmrUtils.sleep(JMRConstants.MAP_TASK_SCHEDULER_SLEEP_MS);
            }
        }
    }

    private static boolean requeueMapTask(final JobInfoInternal jobInfo, final JobExecutionContext<?> jobCtx, final UnassignedMapTasks task,
            final AtomicBoolean stopForError, final String reason, final boolean countAttempt) {
        final boolean requeued = requeueTask(jobInfo, jobCtx.mapAttempts, task.taskId(), stopForError, "MAP", reason, countAttempt,
                () -> jobCtx.enqueueMapTask(task));
        jobCtx.recordEvent((requeued ? "MAP re-queued " : "MAP failed permanently ") + task.taskId() + " (" + reason + ")");
        return requeued;
    }

    private static boolean requeueReduceTask(final JobInfoInternal jobInfo, final JobExecutionContext<?> jobCtx,
            final UnassignedReduceTasks task, final AtomicBoolean stopForError, final String reason, final boolean countAttempt) {
        final boolean requeued = requeueTask(jobInfo, jobCtx.reduceAttempts, task.taskId(), stopForError, "REDUCE", reason, countAttempt,
                () -> jobCtx.enqueueReduceTask(task));
        jobCtx.recordEvent((requeued ? "REDUCE re-queued " : "REDUCE failed permanently ") + task.taskId() + " (" + reason + ")");
        return requeued;
    }

    private static boolean requeueTask(final JobInfoInternal jobInfo, final Map<String, Integer> attempts, final String taskId,
            final AtomicBoolean stopForError, final String phase, final String reason, final boolean countAttempt, final Runnable requeueAction) {
        if (countAttempt) {
            final int attempt = attempts.merge(taskId, 1, Integer::sum);
            if (attempt > JMRConstants.MAX_TASK_RETRIES) {
                stopForError.set(true);
                failJob(jobInfo, phase + " task " + taskId + " exceeded retry limit. Last cause: " + reason);
                return false;
            }
            JMRLog.warn(LOGGER, "{} task {} failed, retry {}/{} ({})", phase, taskId, attempt, JMRConstants.MAX_TASK_RETRIES, reason);
        } else {
            JMRLog.warn(LOGGER, "{} task {} re-queued without consuming a retry ({})", phase, taskId, reason);
        }

        requeueAction.run();
        return true;
    }

    private static void addCompletedMapTask(final JobExecutionContext<?> jobCtx, final CompletedMapTasks completedTask) {
        jobCtx.completedMapTasks.removeIf(existing -> existing.taskId().equals(completedTask.taskId()));
        jobCtx.completedMapTasks.add(completedTask);
        jobCtx.recordEvent("MAP completed " + completedTask.taskId() + " on " + completedTask.worker().getWorkerId());
    }

    private static <O extends Serializable> void addCompletedReduceTask(final JobExecutionContext<O> jobCtx,
            final CompletedReduceTasks<O> completedTask) {
        jobCtx.completedReduceTasks.removeIf(existing -> existing.taskId().equals(completedTask.taskId()));
        jobCtx.completedReduceTasks.add(completedTask);
        jobCtx.recordEvent("REDUCE completed " + completedTask.taskId() + " on " + completedTask.worker().getWorkerId());
    }

    private static <O extends Serializable> byte[] serializeJobResult(final List<CompletedReduceTasks<O>> completedReduceTasks) throws JMRException {
        try {
            final ArrayList<Pair<String, O>> finalResult = completedReduceTasks.stream().sorted(Comparator.comparing(CompletedReduceTasks::partitionId))
                    .flatMap(task -> task.reducedData().stream()).sorted(Comparator.comparing(Pair::getFirst))
                    .collect(Collectors.toCollection(ArrayList::new));
            return JmrUtils.serializeObject(finalResult);
        } catch (IOException e) {
            throw new JMRException("Error serializing job result", e);
        }
    }

    private static List<Pair<Long, Long>> buildChunks(final long dataSize, final long chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }

        final List<Pair<Long, Long>> chunks = new ArrayList<>();
        long start = 0;
        while (start < dataSize) {
            final long end = Math.min(start + chunkSize, dataSize);
            chunks.add(Pair.of(start, end));
            start = end;
        }
        return chunks;
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedMapTasks> tryAssignMapTask(
            final UnassignedMapTasks task, final List<Worker> workers, final List<AssignedMapTasks> assignedTasks,
            final int reducePartitionCount, final JobConfiguration<D, V, O> jobConfig) {
        final Set<Worker> busy = assignedTasks.stream().map(AssignedMapTasks::worker).collect(Collectors.toSet());
        for (Worker worker : workers) {
            if (busy.contains(worker)) {
                continue;
            }

            final Optional<AssignedMapTasks> result = tryAssignMapTask(task, worker, reducePartitionCount, jobConfig);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    static <D extends Serializable, V extends Serializable, O extends Serializable> Optional<AssignedMapTasks> tryAssignMapTask(
            final UnassignedMapTasks task, final Worker worker, final int reducePartitionCount, final JobConfiguration<D, V, O> jobConfig) {
        final boolean assigned = worker.submitMapTask(task.jobId(), task.taskId(), task.chunks().getFirst().intValue(),
                (int) (task.chunks().getSecond() - task.chunks().getFirst()), task.jarId(), reducePartitionCount, jobConfig);
        if (assigned) {
            return Optional.of(task.toAssigned(worker));
        }
        return Optional.empty();
    }

    private static PhaseOutcome cancelExecution(final JobInfoInternal jobInfo) {
        markJobCancelled(jobInfo);
        return PhaseOutcome.CANCELLED;
    }

    private static void markJobCancelled(final JobInfoInternal jobInfo) {
        jobInfo.clearSerializedResult();
        jobInfo.requestCancellation();
        LOGGER.info("<<< Job cancelled: {}", jobInfo.getJobId());
    }

    private static void failJob(final JobInfoInternal jobInfo, final String errorMessage) {
        if (jobInfo.isCancellationRequested()) {
            markJobCancelled(jobInfo);
            return;
        }

        jobInfo.clearSerializedResult();
        jobInfo.setStatus(JobStatus.FAILED);
        jobInfo.setErrorMessage(errorMessage);
    }
}
