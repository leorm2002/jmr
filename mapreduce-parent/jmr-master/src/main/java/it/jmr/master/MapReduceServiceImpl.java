package it.jmr.master;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.stub.StreamObserver;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.grpc.*;

class MapReduceServiceImpl extends MapReduceServiceGrpc.MapReduceServiceImplBase {

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

        // 1 Aggiungo alla mappa dei dei job
        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob(request.getJobId());
        this.jobs.put(jobInfo.getJobId(), jobInfo);

        final String jarPath = this.jarsPaths.get(request.getJarId());

        // Check if the JAR exists, if not return an error
        if (jarPath == null) {
            jobInfo.recievedJarNotFound();
            final SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(false).setMessage("JAR not found: " + request.getJarId())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        jobInfo.recievedJarFound(jarPath);

        final String jobId = request.getJobId();
        final String jobPath = this.jobsPaths.get(jobId);
        // Check if the Job file exists, if not return an error
        if (jobPath == null) {
            jobInfo.recievedSerializedJobNotFound();
            String jobId2 = request.getJobId();
            final SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(false).setMessage("Job file not found: " + jobId2).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
        jobInfo.recievedSerializedJobFound(jobPath);

        System.out.println("Job ricevto: " + jobInfo.toString());

        // Creo la risposta sincrona di accettazione del job
        String jobId4 = request.getJobId();
        SubmitJobResponse response = SubmitJobResponse.newBuilder().setSuccess(true).setJobId(jobId4).setMessage("Job sottomesso con successo")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        // Aggiungi il job alla coda, il thread executor lo prenderà in carico
        jobQueue.add(jobInfo);
    }

    @Override
    public void getJobStatus(GetJobStatusRequest request, StreamObserver<GetJobStatusResponse> responseObserver) {
        JobInfoInternal jobInfo = this.jobs.get(request.getJobId());

        GetJobStatusResponse.Builder responseBuilder = GetJobStatusResponse.newBuilder();

        if (jobInfo != null) {
            responseBuilder.setFound(true).setJobInfo(jobInfo.toProto());
        } else {
            responseBuilder.setFound(false);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listJobs(ListJobsRequest request, StreamObserver<ListJobsResponse> responseObserver) {
        ListJobsResponse.Builder responseBuilder = ListJobsResponse.newBuilder().setTotalCount(this.jobs.size());

        for (JobInfoInternal job : this.jobs.values()) {
            responseBuilder.addJobs(job.toProto());
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void cancelJob(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
        JobInfoInternal jobInfo = jobs.get(request.getJobId());

        CancelJobResponse.Builder responseBuilder = CancelJobResponse.newBuilder();

        if (jobInfo != null && (jobInfo.getStatus() == JobStatus.RUNNING || jobInfo.getStatus() == JobStatus.PENDING)) {
            jobInfo.setStatus(JobStatus.CANCELLED);
            responseBuilder.setSuccess(true).setMessage("Job canceled successfully");
        } else {
            responseBuilder.setSuccess(false).setMessage("Job not found or not cancellable");
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}