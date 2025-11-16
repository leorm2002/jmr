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
     * Checks the health of a specific service
     * 
     * @param serviceName name of the service (e.g. "mypackage.MyService")
     * @return true if the service is SERVING, false otherwise
     */
    private boolean isServerAlive(String serviceName) {
        try {
            final HealthCheckRequest request = HealthCheckRequest.newBuilder().setService(serviceName).build();
            final HealthCheckResponse response = healthStub.withDeadlineAfter(JMRConstants.REQUEST_TIMEOUT_S, TimeUnit.SECONDS).check(request);
            return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING;

        } catch (StatusRuntimeException e) {
            JMRLog.error(LOGGER, "Health check failed for worker {}: {}", workerId, e.getMessage());
            return false;
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error during health check for worker {}: {}", workerId, e.getMessage());
            return false;
        }
    }

    public <D extends Serializable, V extends Serializable, O extends Serializable> boolean submitMapTask(String jobId, String taskId, int offset,
            int limit, String jarPath, JobConfiguration<D, V, O> jobConfig) {
        try {
            JMRLog.info(LOGGER, "Submitting map task {} for job {} to worker {}", taskId, jobId, workerId);
            // 1. upload the jar to the worker
            final String jarId = JarServiceClient.uploadJar(jarPath, jarAsyncStub);
            JMRLog.debug(LOGGER, "Uploaded jar {} to worker {}", jarId, workerId);
            // 2. send the serialized job
            JobServiceClient.uploadJob(jobConfig, jobId, jobAsyncStub);
            JMRLog.debug(LOGGER, "Uploaded job {} to worker {}", jobId, workerId);

            // 3. send the submit
            final it.jmr.grpc.worker.SubmitMapTaskRequest request = it.jmr.grpc.worker.SubmitMapTaskRequest.newBuilder().setTaskId(taskId)
                    .setJobId(jobId).setOffset(offset).setLimit(limit).setJarId(jarId).build();
            final SubmitMapTaskResponse response = stub.submitMapTask(request);
            if(response.getSuccess()) {
                JMRLog.info(LOGGER, "Map task {} for job {} submitted successfully to worker {}", taskId, jobId, workerId);
            } else {
                JMRLog.error(LOGGER, "Failed to submit map task {} for job {} to worker {}", taskId, jobId, workerId);
            }
            return response.getSuccess();
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error submitting map task {} for job {} to worker {}: {}", taskId, jobId, workerId, e.getMessage());
            return false;
        }
    }

    public Pair<WorkerTaskStatus, List<IntermediateLocation>> getMapTaskStatus(String jobId, String taskId) {
        try {
            JMRLog.debug(LOGGER, "Getting status for map task {} of job {} from worker {}", taskId, jobId, workerId);
            final GetMapTaskStatusRequest request = GetMapTaskStatusRequest.newBuilder().setJobId(jobId).setTaskId(taskId).build();
            final GetMapTaskStatusResponse response = stub.getMapTaskStatus(request);

            switch (response.getState()) {
            case TASK_RUNNING:
                return Pair.of(WorkerTaskStatus.RUNNING, Collections.emptyList());
            case TASK_COMPLETED:
                JMRLog.debug(LOGGER, "Map task {} of job {} completed on worker {}", taskId, jobId, workerId);
                return Pair.of(WorkerTaskStatus.COMPLETED, response.getLocationsList().stream()
                        .map(loc -> new IntermediateLocation(loc.getWorkerId(), loc.getTaskId(), loc.getPartitionId())).toList());
            case TASK_MISSING:
                JMRLog.warn(LOGGER, "Map task {} of job {} is missing on worker {}", taskId, jobId, workerId);
                return Pair.of(WorkerTaskStatus.MISSING, Collections.emptyList());
            default:
                JMRLog.error(LOGGER, "Map task {} of job {} on worker {} has failed", taskId, jobId, workerId);
                return Pair.of(WorkerTaskStatus.FAILED, Collections.emptyList());
            }
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error getting status for map task {} of job {} from worker {}: {}", taskId, jobId, workerId, e.getMessage());
            return Pair.of(WorkerTaskStatus.FAILED, Collections.emptyList());
        }
    }
}