package it.jmr.worker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.PartitionInfo;
import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.SubmitMapTaskRequest;
import it.jmr.grpc.worker.SubmitMapTaskResponse;
import it.jmr.grpc.worker.SubmitReduceTaskRequest;
import it.jmr.grpc.worker.SubmitReduceTaskResponse;
import it.jmr.grpc.worker.TaskState;
import it.jmr.grpc.worker.FetchIntermediateDataRequest;
import it.jmr.grpc.worker.GetMapTaskStatusRequest;
import it.jmr.grpc.worker.GetMapTaskStatusResponse;
import it.jmr.grpc.worker.GetWorkerStatusRequest;
import it.jmr.grpc.worker.GetWorkerStatusResponse;
import it.jmr.grpc.worker.HeartbeatRequest;
import it.jmr.grpc.worker.HeartbeatResponse;
import it.jmr.grpc.worker.IntermediateDataChunk;
import it.jmr.grpc.worker.IntermediateDataLocation;
import it.jmr.grpc.worker.WorkerServiceGrpc;
import it.jmr.grpc.worker.WorkerState;
import it.jmr.grpc.worker.WorkerStatus;
import it.jmr.worker.models.TaskResult;
import it.jmr.worker.models.WorkerContext;

class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServiceImpl.class);

    private WorkerContext workerNode;

    WorkerServiceImpl(WorkerContext workerNode) {
        this.workerNode = workerNode;
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

        final TaskResult taskResult = workerNode.taskResults.get(Pair.of(jobId, taskId));
        final List<PartitionInfo> partitions = taskResult.getPartitions();
        final List<IntermediateDataLocation> locations = new LinkedList<>();
        for (final PartitionInfo partitionInfo : partitions) {
            final IntermediateDataLocation loc = IntermediateDataLocation.newBuilder().setWorkerId(workerNode.workerId)
                    .setTaskId(partitionInfo.getPartitionId()).setPartitionId(partitionInfo.getKey()).build();
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

        // If the worker is busy, reject the task
        boolean success;
        if (this.workerNode.busy) {
            JMRLog.warn(LOGGER, "Worker is busy. Rejecting task: {}", request.getTaskId());
            success = false;
        } else {
            // Set the worker as busy
            this.workerNode.busy = true;
            success = true;
        }

        // Send task acceptance response
        final SubmitMapTaskResponse response = SubmitMapTaskResponse.newBuilder().setSuccess(success).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        if (!success) {
            return;
        }

        workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.RUNNING);

        // Start the map task synchronously
        final long startTime = System.currentTimeMillis();
        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        try {
            JMRLog.info(LOGGER, ">>> Starting MAP task execution: {}", taskId);
            final Map<String, List<Serializable>> mappedData = WorkerExecutor.executeMap(request, workerNode);
            JMRLog.info(LOGGER, ">>> Completed MAP task execution: {}", taskId);

            // Save the mapped data to local storage keeping track of partitions
            for (Map.Entry<String, List<Serializable>> entry : mappedData.entrySet()) {
                final String partitionId = entry.getKey();
                final List<Serializable> data = entry.getValue();
                partitionInfos.add(workerNode.intermediateStorage.savePartitionData(taskId, partitionId, data));
            }
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.COMPLETED);

        } catch (JMRException e) {
            JMRLog.error(LOGGER, "Error during MAP task execution: " + taskId, e);
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
        }
        // Set the worker as not busy
        this.workerNode.busy = false;

        final long executionTime = System.currentTimeMillis() - startTime;
        JMRLog.info(LOGGER, "<<< MAP task completed: {} ({}ms)", taskId, executionTime);

        // Save the task info
        final TaskResult taskResult = new TaskResult(jobId, taskId, partitionInfos, executionTime);
        workerNode.taskResults.put(Pair.of(jobId, taskId), taskResult);
    }

    @Override
    public void submitReduceTask(SubmitReduceTaskRequest request, StreamObserver<SubmitReduceTaskResponse> responseObserver) {
        JMRLog.info(LOGGER, "\n>>> Executing REDUCE task: {}", request.getTaskId());
        JMRLog.info(LOGGER, "    Partition: {}", request.getPartitionId());

        String taskId = request.getTaskId();

        // this.workerNode.taskExecutor.submit(() -> {
        // try {
        // long startTime = System.currentTimeMillis();

        // // Deserialize Reducer
        // Reducer<?, ?, ?> reducer = this.workerNode.deserialize(
        // request.getSerializedReducer().toByteArray());

        // // Execute Reduce phase
        // OutputDataLocation outputLocation = executeReducePhase(
        // taskId,
        // request.getPartitionId(),
        // reducer,
        // request.getLocationsList()
        // );

        // long executionTime = System.currentTimeMillis() - startTime;

        // ExecuteReduceTaskResponse response = ExecuteReduceTaskResponse.newBuilder()
        // .setSuccess(true)
        // .setTaskId(taskId)
        // .setOutputLocation(outputLocation)
        // .setExecutionTimeMs(executionTime)
        // .build();

        // JMRLog.info(LOGGER, "<<< REDUCE task completed: {} ({}ms)", taskId, executionTime);

        // responseObserver.onNext(response);
        // responseObserver.onCompleted();

        // } catch (Exception e) {
        // JMRLog.error(LOGGER, "Error in REDUCE task", e);
        // e.printStackTrace();

        // ExecuteReduceTaskResponse response = ExecuteReduceTaskResponse.newBuilder()
        // .setSuccess(false)
        // .setTaskId(taskId)
        // .setErrorMessage(e.getMessage())
        // .build();

        // responseObserver.onNext(response);
        // responseObserver.onCompleted();
        // } finally {
        // this.workerNode.activeTasks.remove(taskId);
        // }
        // });
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

            // Serialize the list into a ByteArrayOutputStream
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(data);
                oos.flush();
            }

            // Convert to byte array
            byte[] serializedData = baos.toByteArray();

            // Send the data in chunks
            int chunkSize = 64 * 1024; // 64KB
            int offset = 0;

            while (offset < serializedData.length) {
                final int length = Math.min(chunkSize, serializedData.length - offset);
                final boolean isLast = (offset + length >= serializedData.length);

                final IntermediateDataChunk chunk = IntermediateDataChunk.newBuilder()
                        .setData(com.google.protobuf.ByteString.copyFrom(serializedData, offset, length)).setIsLast(isLast).build();

                responseObserver.onNext(chunk);
                offset += length;
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
        final WorkerStatus status = WorkerStatus.newBuilder().setActiveTasks(this.workerNode.busy ? 1 : 0).setCpuUsage(99.9).setMemoryUsage(99.9)
                .build();

        final GetWorkerStatusResponse response = GetWorkerStatusResponse.newBuilder().setWorkerId(this.workerNode.workerId)
                .setState(this.workerNode.busy ? WorkerState.BUSY : WorkerState.IDLE).setCurrentStatus(status).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}