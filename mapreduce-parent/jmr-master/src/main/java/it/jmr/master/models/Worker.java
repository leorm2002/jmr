package it.jmr.master.models;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import it.jmr.common.JMRConstants;
import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.models.IntermediateLocation;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.GetMapTaskStatusRequest;
import it.jmr.grpc.worker.GetMapTaskStatusResponse;
import it.jmr.grpc.worker.IntermediateDataLocation;
import it.jmr.grpc.worker.SubmitMapTaskResponse;
import it.jmr.grpc.worker.WorkerServiceGrpc;
import it.jmr.master.WorkerI;

public class Worker {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Worker.class);

    private final String workerId;
    private final String address;
    private final int port;
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;
    private final HealthGrpc.HealthBlockingStub healthStub;

    public Worker(final WorkerI workerI) {
        this.workerId = workerI.workerId();
        this.address = workerI.address();
        this.port = workerI.port();

        // Setup gRPC channel and stubs
        this.channel = io.grpc.ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        this.stub = WorkerServiceGrpc.newBlockingStub(channel);
        this.healthStub = HealthGrpc.newBlockingStub(channel);
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String getWorkerId() {
        return workerId;
    }

    public boolean isAlive() {
        return isServerAlive(JMRConstants.HEALTH_SERVICE_NAME);
    }

    /**
     * Controlla lo stato di salute di un servizio specifico
     * 
     * @param serviceName nome del servizio (es: "mypackage.MyService")
     * @return true se il servizio è SERVING, false altrimenti
     */
    private boolean isServerAlive(String serviceName) {
        try {
            final HealthCheckRequest request = HealthCheckRequest.newBuilder().setService(serviceName).build();
            final HealthCheckResponse response = healthStub.withDeadlineAfter(JMRConstants.REQUEST_TIMEOUT_S, TimeUnit.SECONDS).check(request);
            return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING;

        } catch (StatusRuntimeException e) {
            JMRLog.error(LOGGER, "Health check failed: " + e.getMessage());
            return false;
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error during health check: " + e.getMessage());
            return false;
        }
    }

    public boolean submitMapTask(String jobId, String taskId, int offset, int limit, String jarId) {
        try {
            final it.jmr.grpc.worker.SubmitMapTaskRequest request = it.jmr.grpc.worker.SubmitMapTaskRequest.newBuilder().setTaskId(taskId)
                    .setJobId(jobId).setOffset(offset).setLimit(limit).setJarId(jarId).build();
            final SubmitMapTaskResponse response = stub.submitMapTask(request);
            return response.getSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    public Pair<WorkerTaskStatus, List<IntermediateLocation>> getMapTaskStatus(String jobId, String taskId) {
        final GetMapTaskStatusRequest request = GetMapTaskStatusRequest.newBuilder().setJobId(jobId).setTaskId(taskId).build();
        final GetMapTaskStatusResponse response = stub.getMapTaskStatus(request);

        switch (response.getState()) {
        case TASK_RUNNING:
            return Pair.of(WorkerTaskStatus.RUNNING, Collections.emptyList());
        case TASK_COMPLETED:
            return Pair.of(WorkerTaskStatus.COMPLETED, response.getLocationsList().stream()
                    .map(loc -> new IntermediateLocation(loc.getWorkerId(), loc.getTaskId(), loc.getPartitionId())).toList());
        case TASK_MISSING:
            return Pair.of(WorkerTaskStatus.MISSING, Collections.emptyList());
        default:
            return Pair.of(WorkerTaskStatus.FAILED, Collections.emptyList());
        }
    }
}