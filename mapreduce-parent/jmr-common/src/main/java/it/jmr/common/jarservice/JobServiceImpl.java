package it.jmr.common.jarservice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JmrUtils;
import it.jmr.grpc.JobChunk;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.UploadJobResponse;

public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceImpl.class);
    private final ConcurrentHashMap<String, Path> jobStorage;
    private final Path jobStorageDir;
    private final ResourceUploadedCallback callback;

    public JobServiceImpl(Path jobStorageDir, ConcurrentHashMap<String, Path> jobStorage, ResourceUploadedCallback callback) {
        this.jobStorageDir = jobStorageDir;
        this.jobStorage = jobStorage;
        this.callback = callback;
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
                    final String filename = JmrUtils.isEmpty(jobId) ? JmrUtils.generateJobId() : jobId;
                    final Path filepath = jobStorageDir.resolve(filename);

                    // Track the stored job
                    jobStorage.put(filename, filepath);
                    // Save the file to disk
                    Files.write(filepath, fileData);
                    LOGGER.info("Job file saved to {}", filepath);

                    // Notify via callback
                    callback.onJobUploaded(filename, filepath);

                    // Respond to the client
                    final UploadJobResponse response = UploadJobResponse.newBuilder().setSuccess(true).setMessage("File uploaded via streaming")
                            .setJobId(filename).build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    LOGGER.error("Error saving job file", e);
                    responseObserver.onError(new JMRException("Error saving job file", e));
                }
            }
        };
    }
}
