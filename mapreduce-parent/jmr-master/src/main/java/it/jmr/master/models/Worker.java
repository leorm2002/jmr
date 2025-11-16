package it.jmr.master.models;

import java.io.Serializable;
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
import it.jmr.common.jarservice.JarServiceClient;
import it.jmr.common.jarservice.JobServiceClient;
import it.jmr.common.models.IntermediateLocation;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.worker.GetMapTaskStatusRequest;
import it.jmr.grpc.worker.GetMapTaskStatusResponse;
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
    private final JarServiceGrpc.JarServiceStub jarAsyncStub;
    private final JobServiceGrpc.JobServiceStub jobAsyncStub;

    public Worker(final WorkerI workerI) {
        this.workerId = workerI.workerId();
        this.address = workerI.address();
        this.port = workerI.port();

        // Setup gRPC channel and stubs
        this.channel = io.grpc.ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        this.stub = WorkerServiceGrpc.newBlockingStub(channel);
        this.healthStub = HealthGrpc.newBlockingStub(channel);
        this.jarAsyncStub = JarServiceGrpc.newStub(channel);
        this.jobAsyncStub = JobServiceGrpc.newStub(channel);
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

    public <D extends Serializable, V extends Serializable, O extends Serializable> boolean submitMapTask(String jobId, String taskId, int offset,
            int limit, String jarPath, JobConfiguration<D, V, O> jobConfig) {
        try {

            // 1. carica il jar sul worker
            final String jarId = JarServiceClient.uploadJar(jarPath, jarAsyncStub);
            // 2. invia il job serializzato
            JobServiceClient.uploadJob(jobConfig, jobId, jobAsyncStub);

            // 3. invia il submit
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