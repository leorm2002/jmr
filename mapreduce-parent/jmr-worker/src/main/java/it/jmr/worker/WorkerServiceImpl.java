package it.jmr.worker;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.JMRConstants;
import it.jmr.common.PartitionInfo;
import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.JmrUtils;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.SubmitMapTaskRequest;
import it.jmr.grpc.worker.SubmitMapTaskResponse;
import it.jmr.grpc.worker.SubmitReduceTaskRequest;
import it.jmr.grpc.worker.SubmitReduceTaskResponse;
import it.jmr.grpc.worker.TaskState;
import it.jmr.grpc.worker.CleanupJobDataRequest;
import it.jmr.grpc.worker.CleanupJobDataResponse;
import it.jmr.grpc.worker.FetchIntermediateDataRequest;
import it.jmr.grpc.worker.GetMapTaskStatusRequest;
import it.jmr.grpc.worker.GetMapTaskStatusResponse;
import it.jmr.grpc.worker.GetMapTaskStatusResponseIntermediateDataLocation;
import it.jmr.grpc.worker.GetWorkerStatusRequest;
import it.jmr.grpc.worker.GetWorkerStatusResponse;
import it.jmr.grpc.worker.HeartbeatRequest;
import it.jmr.grpc.worker.HeartbeatResponse;
import it.jmr.grpc.worker.IntermediateDataChunk;
import it.jmr.grpc.worker.ReducedData;
import it.jmr.grpc.worker.WorkerServiceGrpc;
import it.jmr.grpc.worker.WorkerState;
import it.jmr.grpc.worker.WorkerStatus;
import it.jmr.worker.models.TaskResult;
import it.jmr.worker.models.WorkerContext;
import it.jmr.worker.models.ReduceTaskResult;

class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServiceImpl.class);

    private WorkerContext workerNode;
    private WorkerServer workerServer;

    WorkerServiceImpl(WorkerContext workerNode, WorkerServer workerServer) {
        this.workerNode = workerNode;
        this.workerServer = workerServer;
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        JMRLog.trace(LOGGER, "Received heartbeat from {}", request.getWorkerId());
        final HeartbeatResponse response = HeartbeatResponse.newBuilder().setOk(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getMapTaskStatus(GetMapTaskStatusRequest request, StreamObserver<GetMapTaskStatusResponse> responseObserver) {

        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();
        JMRLog.debug(LOGGER, "Received getMapTaskStatus request for task {} of job {}", taskId, jobId);

        final WorkerTaskStatus status = workerNode.statusMap.getOrDefault(Pair.of(jobId, taskId), WorkerTaskStatus.MISSING);

        if (status == WorkerTaskStatus.MISSING) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_MISSING).build());
            responseObserver.onCompleted();
            JMRLog.warn(LOGGER, "Map task {} for job {} is missing", taskId, jobId);
            return;
        }

        if (status == WorkerTaskStatus.RUNNING) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_RUNNING).build());
            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "Map task {} for job {} is running", taskId, jobId);
            return;
        }

        if (status == WorkerTaskStatus.FAILED) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            JMRLog.error(LOGGER, "Map task {} for job {} has failed", taskId, jobId);
            return;
        }

        final TaskResult taskResult = workerNode.mapTaskResults.get(Pair.of(jobId, taskId));

        // Safety check in case status is COMPLETED but result is missing (race
        // condition or error)
        if (taskResult == null) {
            JMRLog.error(LOGGER, "Status is COMPLETED but result is missing for task {}", taskId);
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            return;
        }

        final List<PartitionInfo> partitions = taskResult.getPartitions();
        final List<GetMapTaskStatusResponseIntermediateDataLocation> locations = new LinkedList<>();
        for (final PartitionInfo partitionInfo : partitions) {
            final GetMapTaskStatusResponseIntermediateDataLocation loc = GetMapTaskStatusResponseIntermediateDataLocation.newBuilder()
                    .setWorkerId(workerNode.workerId).setTaskId(partitionInfo.getPartitionId()).setPartitionId(partitionInfo.getKey()).build();
            locations.add(loc);
        }
        JMRLog.debug(LOGGER, "Map task {} for job {} is completed", taskId, jobId);

        responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().addAllLocations(locations).setState(TaskState.TASK_COMPLETED).build());
        responseObserver.onCompleted();
    }

    @Override
    public void submitMapTask(SubmitMapTaskRequest request, StreamObserver<SubmitMapTaskResponse> responseObserver) {
        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();

        JMRLog.info(LOGGER, "\n>>> Received MAP task: {} for job: {}", taskId, jobId);
        workerNode.recordEvent("MAP received " + taskId + " for job " + jobId);
        workerNode.rememberJobJar(jobId, request.getJarId());

        // 1. Controllo Busy e Rifiuto Immediato
        if (!this.workerNode.tryAcquireTaskSlot()) {
            JMRLog.warn(LOGGER, "Worker is busy. Rejecting MAP task: {}", taskId);
            responseObserver.onNext(SubmitMapTaskResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        // 2. Accettazione Task
        // Impostiamo subito lo stato a RUNNING così i primi heartbeat rileveranno il
        // lavoro
        workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.RUNNING);

        // 3. Risposta gRPC immediata (ACK)
        // Il Master riceve "true" che significa "Ho accettato il lavoro", non "Ho
        // finito".
        final SubmitMapTaskResponse response = SubmitMapTaskResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // 4. Esecuzione Asincrona (Non blocca il thread gRPC)
        CompletableFuture.runAsync(() -> {
            final long startTime = System.currentTimeMillis();
            try {
                JMRLog.info(LOGGER, ">>> Starting ASYNC MAP execution: {}", taskId);

                final Map<String, List<Serializable>> mappedData = WorkerExecutor.executeMap(request, workerNode);

                // Salvataggio partizioni
                final List<PartitionInfo> partitionInfos = new ArrayList<>();
                for (Map.Entry<String, List<Serializable>> entry : mappedData.entrySet()) {
                    final String partitionId = entry.getKey();
                    final List<Serializable> data = entry.getValue();
                    partitionInfos.add(workerNode.intermediateStorage.savePartitionData(taskId, partitionId, data));
                }

                final long executionTime = System.currentTimeMillis() - startTime;

                // Aggiornamento risultati e stato
                final TaskResult taskResult = new TaskResult(jobId, taskId, partitionInfos, executionTime);
                workerNode.mapTaskResults.put(Pair.of(jobId, taskId), taskResult);
                workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.COMPLETED);
                workerNode.recordEvent("MAP completed " + taskId + " in " + executionTime + "ms");

                JMRLog.info(LOGGER, "<<< MAP task completed: {} ({}ms)", taskId, executionTime);

            } catch (Exception e) {
                JMRLog.error(LOGGER, "Error during ASYNC MAP execution: {}", taskId, e);
                JMRLog.error(LOGGER, "{}", e);
                workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
                workerNode.recordEvent("MAP failed " + taskId + ": " + e.getClass().getSimpleName());
            } finally {
                // Rilascia il worker per nuovi task
                this.workerNode.releaseTaskSlot();
            }
        }, workerNode.taskExecutor).exceptionally(throwable -> {
            JMRLog.error(LOGGER, "Async MAP execution pipeline failed for {}", taskId, throwable);
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
            workerNode.recordEvent("MAP failed " + taskId + ": " + throwable.getClass().getSimpleName());
            workerNode.releaseTaskSlot();
            return null;
        });
    }

    @Override
    public void submitReduceTask(SubmitReduceTaskRequest request, StreamObserver<SubmitReduceTaskResponse> responseObserver) {
        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();

        JMRLog.info(LOGGER, "\n>>> Received REDUCE task: {} for job: {}", taskId, jobId);
        JMRLog.info(LOGGER, "    Partition: {}", request.getPartitionId());
        workerNode.recordEvent("REDUCE received " + taskId + " for partition " + request.getPartitionId());
        workerNode.rememberJobJar(jobId, request.getJarId());

        // 1. Controllo Busy
        if (!this.workerNode.tryAcquireTaskSlot()) {
            JMRLog.warn(LOGGER, "Worker is busy. Rejecting REDUCE task: {}", taskId);
            responseObserver.onNext(
                    SubmitReduceTaskResponse.newBuilder().setSuccess(false).setTaskId(taskId).setErrorMessage(JMRConstants.WORKER_BUSY).build());
            responseObserver.onCompleted();
            return;
        }

        // 2. Accettazione Task
        workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.RUNNING);

        // 3. Risposta gRPC immediata (ACK)
        // Nota: executionTimeMs è 0 qui perché il task non è ancora finito. Il master
        // lo prenderà dallo Status.
        final SubmitReduceTaskResponse response = SubmitReduceTaskResponse.newBuilder().setSuccess(true).setTaskId(taskId).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // 4. Esecuzione Asincrona
        CompletableFuture.runAsync(() -> {
            final long startTime = System.currentTimeMillis();
            try {
                JMRLog.info(LOGGER, ">>> Starting ASYNC REDUCE execution: {}", taskId);

                List<Pair<String, Serializable>> reducedData = WorkerExecutor.executeReduce(request, workerNode, workerServer);

                final long executionTime = System.currentTimeMillis() - startTime;

                // Aggiornamento risultati e stato
                workerNode.reduceResultStorage.saveReducedData(jobId, taskId, reducedData);
                final ReduceTaskResult taskResult = new ReduceTaskResult(jobId, taskId, reducedData.size(), executionTime);
                workerNode.reduceTaskResults.put(Pair.of(jobId, taskId), taskResult);
                workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.COMPLETED);
                workerNode.recordEvent("REDUCE completed " + taskId + " in " + executionTime + "ms");

                JMRLog.info(LOGGER, "<<< REDUCE task completed: {} ({}ms)", taskId, executionTime);

            } catch (Exception e) {
                JMRLog.error(LOGGER, "Error in ASYNC REDUCE task: {}", taskId);
                JMRLog.error(LOGGER, "{}", e);
                workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
                workerNode.recordEvent("REDUCE failed " + taskId + ": " + e.getClass().getSimpleName());
            } finally {
                // Rilascia il worker
                this.workerNode.releaseTaskSlot();
            }
        }, workerNode.taskExecutor).exceptionally(throwable -> {
            JMRLog.error(LOGGER, "Async REDUCE execution pipeline failed for {}", taskId, throwable);
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
            workerNode.recordEvent("REDUCE failed " + taskId + ": " + throwable.getClass().getSimpleName());
            workerNode.releaseTaskSlot();
            return null;
        });
    }

    @Override
    public void getReduceTaskStatus(it.jmr.grpc.worker.GetReduceTaskStatusRequest request,
            StreamObserver<it.jmr.grpc.worker.GetReduceTaskStatusResponse> responseObserver) {
        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();
        JMRLog.debug(LOGGER, "Received getReduceTaskStatus request for task {} of job {}", taskId, jobId);

        final WorkerTaskStatus status = workerNode.statusMap.getOrDefault(Pair.of(jobId, taskId), WorkerTaskStatus.MISSING);

        if (status == WorkerTaskStatus.MISSING) {
            responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_MISSING).build());
            responseObserver.onCompleted();
            JMRLog.warn(LOGGER, "Reduce task {} for job {} is missing", taskId, jobId);
            return;
        }

        if (status == WorkerTaskStatus.RUNNING) {
            responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_RUNNING).build());
            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "Reduce task {} for job {} is running", taskId, jobId);
            return;
        }

        if (status == WorkerTaskStatus.FAILED) {
            responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            JMRLog.error(LOGGER, "Reduce task {} for job {} has failed", taskId, jobId);
            return;
        }

        final ReduceTaskResult taskResult = workerNode.reduceTaskResults.get(Pair.of(jobId, taskId));

        // Safety check
        if (taskResult == null) {
            JMRLog.error(LOGGER, "Status is COMPLETED but result is missing for reduce task {}", taskId);
            responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            return;
        }

        final List<Pair<String, Serializable>> storedReducedData = workerNode.reduceResultStorage.getReducedData(jobId, taskId);
        if (storedReducedData == null) {
            JMRLog.error(LOGGER, "Status is COMPLETED but persisted result is missing for reduce task {}", taskId);
            responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            return;
        }

        final List<it.jmr.grpc.worker.ReducedData> reducedDataList = new ArrayList<>();
        try {
            for (final Pair<String, Serializable> entry : storedReducedData) {
                final ReducedData pair = it.jmr.grpc.worker.ReducedData.newBuilder().setKey(entry.getFirst())
                        .setValue(com.google.protobuf.ByteString.copyFrom(JmrUtils.serializeObject(entry.getSecond()))).build();
                reducedDataList.add(pair);
            }
        } catch (IOException e) {
            JMRLog.error(LOGGER, "Error serializing reduced data for task {} job {}", taskId, jobId, e);
            responseObserver.onError(e);
            return;
        }

        JMRLog.debug(LOGGER, "Reduce task {} for job {} is completed", taskId, jobId);

        responseObserver.onNext(it.jmr.grpc.worker.GetReduceTaskStatusResponse.newBuilder().setState(TaskState.TASK_COMPLETED)
                .addAllReducedData(reducedDataList).build());
        responseObserver.onCompleted();
    }

    @Override
    /**
     * Retrieves the intermediate data produced by the Map phase for a specific
     * partition.
     */
    public void fetchIntermediateData(FetchIntermediateDataRequest request, StreamObserver<IntermediateDataChunk> responseObserver) {
        JMRLog.debug(LOGGER, "Fetching intermediate data for task {} partition {}", request.getTaskId(), request.getPartitionId());
        try {
            final List<Serializable> data = workerNode.intermediateStorage.getPartitionData(request.getTaskId(), request.getPartitionId());
            final int batchSize = Math.max(1, JMRConstants.INTERMEDIATE_DATA_BATCH_SIZE);
            final int totalBatches = Math.max(1, (data.size() + batchSize - 1) / batchSize);

            for (int batchStart = 0; batchStart < data.size(); batchStart += batchSize) {
                final int batchEnd = Math.min(batchStart + batchSize, data.size());
                final List<Serializable> batch = new ArrayList<>(data.subList(batchStart, batchEnd));
                final byte[] serializedBatch = JmrUtils.gzip(JmrUtils.serializeObject((Serializable) batch));
                final boolean isLast = batchEnd >= data.size();

                final IntermediateDataChunk chunk = IntermediateDataChunk.newBuilder().setData(com.google.protobuf.ByteString.copyFrom(serializedBatch))
                        .setIsLast(isLast).build();

                responseObserver.onNext(chunk);
                JMRLog.debug(LOGGER, "Fetched intermediate batch {}/{} for task {} partition {}", (batchStart / batchSize) + 1, totalBatches,
                        request.getTaskId(), request.getPartitionId());
            }

            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "Finished fetching intermediate data for task {} partition {}", request.getTaskId(), request.getPartitionId());

        } catch (IOException e) {
            JMRLog.error(LOGGER, "Error fetching intermediate data for task {} partition {}", request.getTaskId(), request.getPartitionId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getWorkerStatus(GetWorkerStatusRequest request, StreamObserver<GetWorkerStatusResponse> responseObserver) {
        JMRLog.debug(LOGGER, "Received getWorkerStatus request for worker {}", request.getWorkerId());
        final Runtime runtime = Runtime.getRuntime();
        final long usedBytes = runtime.totalMemory() - runtime.freeMemory();
        final long maxBytes = runtime.maxMemory();
        final double heapUsagePct = maxBytes > 0 ? (usedBytes * 100.0d / maxBytes) : 0.0d;
        final WorkerStatus status = WorkerStatus.newBuilder().setActiveTasks(this.workerNode.isBusy() ? 1 : 0).setCpuUsage(99.9)
                .setMemoryUsage(heapUsagePct)
                .build();

        final GetWorkerStatusResponse response = GetWorkerStatusResponse.newBuilder().setWorkerId(this.workerNode.workerId)
                .setState(this.workerNode.isBusy() ? WorkerState.BUSY : WorkerState.IDLE).setCurrentStatus(status).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void cleanupJobData(final CleanupJobDataRequest request, final StreamObserver<CleanupJobDataResponse> responseObserver) {
        final String jobId = request.getJobId();
        JMRLog.info(LOGGER, "Cleaning local worker data for job {}", jobId);

        try {
            cleanupJobArtifacts(jobId);
            workerNode.recordEvent("CLEANUP completed for job " + jobId);
            responseObserver.onNext(CleanupJobDataResponse.newBuilder().setSuccess(true).setMessage("Cleanup completed").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error cleaning local worker data for job {}", jobId, e);
            responseObserver.onNext(CleanupJobDataResponse.newBuilder().setSuccess(false).setMessage(e.getMessage()).build());
            responseObserver.onCompleted();
        }
    }

    private void cleanupJobArtifacts(final String jobId) throws IOException {
        final List<Pair<String, String>> mapKeys = workerNode.mapTaskResults.keySet().stream().filter(key -> key.getFirst().equals(jobId)).toList();
        for (final Pair<String, String> key : mapKeys) {
            workerNode.mapTaskResults.remove(key);
            workerNode.intermediateStorage.deleteTaskData(key.getSecond());
        }

        final List<Pair<String, String>> reduceKeys = workerNode.reduceTaskResults.keySet().stream().filter(key -> key.getFirst().equals(jobId)).toList();
        for (final Pair<String, String> key : reduceKeys) {
            workerNode.reduceTaskResults.remove(key);
            workerNode.reduceResultStorage.deleteTaskResult(jobId, key.getSecond());
        }

        workerNode.statusMap.keySet().removeIf(key -> key.getFirst().equals(jobId));

        final Path jobFile = workerNode.jobStorage.remove(jobId);
        if (jobFile != null) {
            Files.deleteIfExists(jobFile);
        }

        final String jarId = workerNode.forgetJobJar(jobId);
        if (jarId != null && !workerNode.isJarStillReferenced(jarId)) {
            final Path jarFile = workerNode.jarStorage.remove(jarId);
            if (jarFile != null) {
                Files.deleteIfExists(jarFile);
            }
        }
    }
}
