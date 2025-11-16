package it.jmr.common.jarservice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import it.jmr.common.JMRConstants;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.JMRLog;
import it.jmr.grpc.JobChunk;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.UploadJobResponse;

public class JobServiceClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceClient.class);

    public static <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig,
            JobServiceGrpc.JobServiceStub jobAsyncStub) throws JMRException, InterruptedException {
        return uploadJob(jobConfig, null, jobAsyncStub);
    }

    public static <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig,
            String jobId, JobServiceGrpc.JobServiceStub jobAsyncStub) throws JMRException, InterruptedException {

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicReference<String> returnedJobId = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();

        StreamObserver<UploadJobResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(UploadJobResponse response) {
                returnedJobId.set(response.getJobId());
                JMRLog.info(LOGGER, "Job uploaded successfully - ID: {} - {}", response.getJobId(), response.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                exception.set(new JMRException("Error during job upload", t));
                JMRLog.error(LOGGER, "Error during job upload: {}", t.getMessage(), t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                JMRLog.debug(LOGGER, "Job upload completed");
                finishLatch.countDown();
            }
        };

        StreamObserver<JobChunk> requestObserver = jobAsyncStub.uploadJob(responseObserver);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            JMRLog.debug(LOGGER, "Serializing job configuration...");
            oos.writeObject(jobConfig);
            oos.flush();

            byte[] jobBytes = baos.toByteArray();
            long totalSize = jobBytes.length;
            long uploadedBytes = 0;

            JMRLog.info(LOGGER, "Starting job upload ({} bytes)", totalSize);

            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(jobBytes)) {
                byte[] buffer = new byte[JMRConstants.UPLOAD_CHUNK_SIZE];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    var b = JobChunk.newBuilder().setContent(ByteString.copyFrom(buffer, 0, bytesRead));
                    if (jobId != null) {
                        b.setJobId(jobId);
                    }
                    JobChunk chunk = b.build();

                    requestObserver.onNext(chunk);
                    uploadedBytes += bytesRead;

                    int percentage = (int) ((uploadedBytes * 100) / totalSize);
                    if (percentage % 25 == 0 || uploadedBytes == totalSize) {
                        JMRLog.debug(LOGGER, "Job upload progress: {}%", percentage);
                    }
                }
            }

            requestObserver.onCompleted();
            JMRLog.info(LOGGER, "Job upload complete, waiting for confirmation...");

        } catch (IOException e) {
            JMRLog.error(LOGGER, "Error during job serialization or upload: {}", e.getMessage(), e);
            requestObserver.onError(e);
            throw new JMRException("Error during job upload", e);
        }

        if (!finishLatch.await(JMRConstants.JOB_UPLOAD_TIMEOUT_M, TimeUnit.MINUTES)) {
            JMRLog.error(LOGGER, "Timeout during job upload after {} minute(s)", JMRConstants.JOB_UPLOAD_TIMEOUT_M);
            throw new JMRException("Timeout during job upload");
        }

        if (exception.get() != null) {
            throw new JMRException(exception.get().getMessage(), exception.get());
        }

        if (returnedJobId.get() == null) {
            JMRLog.error(LOGGER, "Upload completed but no Job ID received");
            throw new JMRException("No Job ID received from server");
        }

        return returnedJobId.get();
    }
}
