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

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import it.jmr.common.JMRConstants;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.JMRLog;
import it.jmr.grpc.JobChunk;
import it.jmr.grpc.JobServiceGrpc;
import it.jmr.grpc.UploadJobResponse;

public class JobServiceClient {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(JobServiceClient.class);

    public static <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig,
            JobServiceGrpc.JobServiceStub jobAsyncStub) throws IOException, InterruptedException {
        return uploadJob(jobConfig, null, jobAsyncStub);
    }

    public static <D extends Serializable, V extends Serializable, O extends Serializable> String uploadJob(JobConfiguration<D, V, O> jobConfig,
            String jobId, JobServiceGrpc.JobServiceStub jobAsyncStub) throws IOException, InterruptedException {

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicReference<String> returnedJobId = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();

        StreamObserver<UploadJobResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(UploadJobResponse response) {
                returnedJobId.set(response.getJobId());
                JMRLog.info(LOGGER, "Job caricato con successo - ID: {} - {}", response.getJobId(), response.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                exception.set(new IOException("Errore durante il caricamento del job", t));
                JMRLog.error(LOGGER, "Errore durante il caricamento del job: {}", t.getMessage(), t);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                JMRLog.debug(LOGGER, "Upload job completato");
                finishLatch.countDown();
            }
        };

        StreamObserver<JobChunk> requestObserver = jobAsyncStub.uploadJob(responseObserver);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            JMRLog.debug(LOGGER, "Serializzazione configurazione job in corso...");
            oos.writeObject(jobConfig);
            oos.flush();

            byte[] jobBytes = baos.toByteArray();
            long totalSize = jobBytes.length;
            long uploadedBytes = 0;

            JMRLog.info(LOGGER, "Inizio caricamento job ({} bytes)", totalSize);

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
                        JMRLog.debug(LOGGER, "Progresso upload job: {}%", percentage);
                    }
                }
            }

            requestObserver.onCompleted();
            JMRLog.info(LOGGER, "Invio job completato, in attesa di conferma...");

        } catch (IOException e) {
            JMRLog.error(LOGGER, "Errore durante la serializzazione o l'invio del job: {}", e.getMessage(), e);
            requestObserver.onError(e);
            throw new IOException("Errore durante l'upload del job", e);
        }

        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            JMRLog.error(LOGGER, "Timeout durante il caricamento del job dopo 1 minuto");
            throw new IOException("Timeout durante il caricamento del job");
        }

        if (exception.get() != null) {
            throw new IOException(exception.get());
        }

        if (returnedJobId.get() == null) {
            JMRLog.error(LOGGER, "Caricamento completato ma nessun Job ID ricevuto");
            throw new IOException("Nessun Job ID ricevuto dal server");
        }

        return returnedJobId.get();
    }
}
