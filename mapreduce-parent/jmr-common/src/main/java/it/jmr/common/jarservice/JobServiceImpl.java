package it.jmr.common.jarservice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.stub.StreamObserver;
import it.jmr.grpc.JobChunk;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.UploadJobResponse;

public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private ConcurrentHashMap<String, String> jobStorage;
    private String jobStorageDir;

    public JobServiceImpl(String jobStorageDir, ConcurrentHashMap<String, String> jobStorage) {
        this.jobStorageDir = jobStorageDir;
        this.jobStorage = jobStorage;
    }

    @Override
    public StreamObserver<JobChunk> uploadJob(StreamObserver<UploadJobResponse> responseObserver) {

        return new StreamObserver<JobChunk>() {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            String jobId = null;

            @Override
            public void onNext(JobChunk chunk) {
                if (jobId == null) {
                    jobId = chunk.getJobId();
                }
                try {
                    outputStream.write(chunk.getContent().toByteArray());
                } catch (IOException e) {
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                try {
                    final byte[] fileData = outputStream.toByteArray();
                    final String filename = jobId == null || jobId.isEmpty() ? UUID.randomUUID().toString() : jobId;
                    final Path filepath = Paths.get(jobStorageDir, filename);

                    jobStorage.put(filename, filepath.toString());
                    // Save the file to disk
                    Files.write(filepath, fileData);

                    // Rispondi al client
                    final UploadJobResponse response = UploadJobResponse.newBuilder().setSuccess(true).setMessage("File caricato via streaming")
                            .setJobId(filename).build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (IOException e) {
                    responseObserver.onError(e);
                }
            }
        };
    }
}