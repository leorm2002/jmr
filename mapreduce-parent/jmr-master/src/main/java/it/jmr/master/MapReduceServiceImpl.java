package it.jmr.master;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.grpc.*;

class MapReduceServiceImpl extends MapReduceServiceGrpc.MapReduceServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceServiceImpl.class);

    private final ConcurrentHashMap<String, String> jarsPaths;
    private final ConcurrentHashMap<String, JobInfoInternal> jobs;
    private final Queue<JobInfoInternal> jobQueue;
    private final ConcurrentHashMap<String, String> jobsPaths;

    MapReduceServiceImpl(ConcurrentHashMap<String, String> jarsPaths, ConcurrentHashMap<String, String> jobsPaths, Queue<JobInfoInternal> jobQueue) {
        this.jobs = new ConcurrentHashMap<>();
        this.jarsPaths = jarsPaths;
        this.jobQueue = jobQueue;
        this.jobsPaths = jobsPaths;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        final String jarId = request.getJarId();
        final String jobId = request.getJobId();
        LOGGER.info("Received job submission request for jarId: {} and jobId: {}", jarId, jobId);

        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob(jobId);
        this.jobs.put(jobInfo.getJobId(), jobInfo);

        final String jarPath = this.jarsPaths.get(jarId);

        if (jarPath == null) {
            jobInfo.recievedJarNotFound();
            LOGGER.error("JAR not found: {}", jarId);
            final SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(false).setMessage("JAR not found: " + jarId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        jobInfo.recievedJarFound(jarPath, jarId);
        LOGGER.debug("JAR found for job {}: {}", jobInfo.getJobId(), jarPath);

        final String jobPath = this.jobsPaths.get(jobId);
        if (jobPath == null) {
            jobInfo.recievedSerializedJobNotFound();
            LOGGER.error("Job file not found: {}", jobId);
            final SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(false).setMessage("Job file not found: " + jobId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        jobInfo.recievedSerializedJobFound(jobPath);
        LOGGER.debug("Job file found for job {}: {}", jobInfo.getJobId(), jobPath);

        LOGGER.info("Job received: {}", jobInfo.toString());

        SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(true).setJobId(jobId).setMessage("Job submitted successfully").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        LOGGER.info("Adding job {} to the queue", jobInfo.getJobId());
        jobQueue.add(jobInfo);
    }

    @Override
    public void getJobStatus(GetJobStatusRequest request, StreamObserver<GetJobStatusResponse> responseObserver) {
        LOGGER.debug("Received getJobStatus request for jobId: {}", request.getJobId());
        JobInfoInternal jobInfo = this.jobs.get(request.getJobId());

        GetJobStatusResponse.Builder responseBuilder = GetJobStatusResponse.newBuilder();

        if (jobInfo != null) {
            LOGGER.debug("Job {} found with status: {}", request.getJobId(), jobInfo.getStatus());
            responseBuilder.setFound(true).setJobInfo(jobInfo.toProto());
        } else {
            LOGGER.warn("Job {} not found", request.getJobId());
            responseBuilder.setFound(false);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listJobs(ListJobsRequest request, StreamObserver<ListJobsResponse> responseObserver) {
        LOGGER.info("Received listJobs request");
        ListJobsResponse.Builder responseBuilder = ListJobsResponse.newBuilder().setTotalCount(this.jobs.size());

        for (JobInfoInternal job : this.jobs.values()) {
            responseBuilder.addJobs(job.toProto());
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void cancelJob(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
        LOGGER.info("Received cancelJob request for jobId: {}", request.getJobId());
        JobInfoInternal jobInfo = jobs.get(request.getJobId());

        CancelJobResponse.Builder responseBuilder = CancelJobResponse.newBuilder();

        if (jobInfo != null && (jobInfo.getStatus() == JobStatus.RUNNING || jobInfo.getStatus() == JobStatus.PENDING)) {
            jobInfo.setStatus(JobStatus.CANCELLED);
            LOGGER.info("Job {} cancelled successfully", request.getJobId());
            responseBuilder.setSuccess(true).setMessage("Job canceled successfully");
        } else {
            LOGGER.warn("Job {} not found or not cancellable", request.getJobId());
            responseBuilder.setSuccess(false).setMessage("Job not found or not cancellable");
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}