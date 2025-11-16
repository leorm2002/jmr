package it.jmr.common.jarservice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.exceptions.JMRException;
import it.jmr.grpc.JobChunk;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.UploadJobResponse;

public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);
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
                    LOGGER.error("Error writing job chunk to stream", e);
                    onError(new JMRException("Error writing job chunk to stream", e));
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.error("Error during job upload", t);
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
                    LOGGER.info("Job file saved to {}", filepath);

                    // Respond to the client
                    final UploadJobResponse response = UploadJobResponse.newBuilder().setSuccess(true).setMessage("File uploaded via streaming")
                            .setJobId(filename).build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (IOException e) {
                    LOGGER.error("Error saving job file", e);
                    responseObserver.onError(new JMRException("Error saving job file", e));
                }
            }
        };
    }
}