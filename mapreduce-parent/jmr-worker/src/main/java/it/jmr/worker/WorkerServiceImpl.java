package it.jmr.worker;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
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
        final HeartbeatResponse response = HeartbeatResponse.newBuilder().setOk(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getMapTaskStatus(GetMapTaskStatusRequest request, StreamObserver<GetMapTaskStatusResponse> responseObserver) {

        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();

        final WorkerTaskStatus status = workerNode.statusMap.getOrDefault(Pair.of(jobId, taskId), WorkerTaskStatus.MISSING);

        if (status == WorkerTaskStatus.MISSING) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_MISSING).build());
            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "    Task MAP mancante: " + taskId + " per job: " + jobId);
            return;
        }

        if (status == WorkerTaskStatus.RUNNING) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_RUNNING).build());
            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "    Task MAP in esecuzione: " + taskId + " per job: " + jobId);
            return;
        }

        if (status == WorkerTaskStatus.FAILED) {
            responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().setState(TaskState.TASK_FAILED).build());
            responseObserver.onCompleted();
            JMRLog.debug(LOGGER, "    Task MAP fallito: " + taskId + " per job: " + jobId);
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
        JMRLog.debug(LOGGER, "    Task MAP completato: " + taskId + " per job: " + jobId);

        responseObserver.onNext(GetMapTaskStatusResponse.newBuilder().addAllLocations(locations).setState(TaskState.TASK_COMPLETED).build());
        responseObserver.onCompleted();

    }

    @Override
    public void submitMapTask(SubmitMapTaskRequest request, StreamObserver<SubmitMapTaskResponse> responseObserver) {
        final String jobId = request.getJobId();
        final String taskId = request.getTaskId();

        JMRLog.debug(LOGGER, "\n>>> Ricevuto MAP task: " + taskId + " per job: " + jobId);

        // Se il worker è occupato, rifiuta il task
        boolean success;
        if (this.workerNode.busy) {
            JMRLog.error(LOGGER, "    Worker occupato. Rifiutato il task: " + request.getTaskId());
            success = false;
        } else {
            // Imposto come busy il worker
            this.workerNode.busy = true;
            success = true;
        }

        // Invia risposta di accettazione del task
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
            JMRLog.debug(LOGGER, ">>> Inizio Esecuzione MAP task: " + taskId);
            final Map<String, List<Serializable>> mappedData = WorkerExecutor.executeMap(request, workerNode);
            JMRLog.debug(LOGGER, ">>> Completata Esecuzione MAP task: " + taskId);

            // Save the mapped data to local storage keeping track of partitions
            for (Map.Entry<String, List<Serializable>> entry : mappedData.entrySet()) {
                final String partitionId = entry.getKey();
                final List<Serializable> data = entry.getValue();
                partitionInfos.add(workerNode.intermediateStorage.savePartitionData(taskId, partitionId, data));
            }
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.COMPLETED);

        } catch (Exception e) {
            JMRLog.error(LOGGER, "Errore durante l'esecuzione del MAP task: " + taskId + ". " + e.getMessage());
            workerNode.statusMap.put(Pair.of(jobId, taskId), WorkerTaskStatus.FAILED);
        }
        // Set the worker as not busy
        this.workerNode.busy = false;

        final long executionTime = System.currentTimeMillis() - startTime;
        JMRLog.debug(LOGGER, "<<< MAP task completato: " + taskId + " (" + executionTime + "ms)");

        // Save the task info
        final TaskResult taskResult = new TaskResult(jobId, taskId, partitionInfos, executionTime);
        workerNode.taskResults.put(Pair.of(jobId, taskId), taskResult);
    }

    @Override
    public void submitReduceTask(SubmitReduceTaskRequest request, StreamObserver<SubmitReduceTaskResponse> responseObserver) {
        System.out.println("\n>>> Esecuzione REDUCE task: " + request.getTaskId());
        System.out.println("    Partition: " + request.getPartitionId());

        String taskId = request.getTaskId();

        // this.workerNode.taskExecutor.submit(() -> {
        // try {
        // long startTime = System.currentTimeMillis();

        // // Deserializza Reducer
        // Reducer<?, ?, ?> reducer = this.workerNode.deserialize(
        // request.getSerializedReducer().toByteArray());

        // // Esegui fase Reduce
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

        // System.out.println("<<< REDUCE task completato: " + taskId +
        // " (" + executionTime + "ms)");

        // responseObserver.onNext(response);
        // responseObserver.onCompleted();

        // } catch (Exception e) {
        // System.err.println("Errore REDUCE task: " + e.getMessage());
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
     * Recupera i dati intermedi prodotti dalla fase di Map per una specifica
     * partizione.
     */
    public void fetchIntermediateData(FetchIntermediateDataRequest request, StreamObserver<IntermediateDataChunk> responseObserver) {
        try {
            final List<Serializable> data = workerNode.intermediateStorage.getPartitionData(request.getTaskId(), request.getPartitionId());

            // Serializza la lista in un ByteArrayOutputStream
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(data);
                oos.flush();
            }

            // Converti in array di byte
            byte[] serializedData = baos.toByteArray();

            // Invia i dati in chunk
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

        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getWorkerStatus(GetWorkerStatusRequest request, StreamObserver<GetWorkerStatusResponse> responseObserver) {
        final WorkerStatus status = WorkerStatus.newBuilder().setActiveTasks(this.workerNode.busy ? 1 : 0).setCpuUsage(99.9).setMemoryUsage(99.9)
                .build();

        final GetWorkerStatusResponse response = GetWorkerStatusResponse.newBuilder().setWorkerId(this.workerNode.workerId)
                .setState(this.workerNode.busy ? WorkerState.BUSY : WorkerState.IDLE).setCurrentStatus(status).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}