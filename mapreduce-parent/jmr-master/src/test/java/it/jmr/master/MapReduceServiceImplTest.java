package it.jmr.master;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.junit.jupiter.api.Test;

import io.grpc.stub.StreamObserver;
import it.jmr.grpc.CancelJobRequest;
import it.jmr.grpc.CancelJobResponse;
import it.jmr.grpc.GetJobStatusRequest;
import it.jmr.grpc.GetJobStatusResponse;
import it.jmr.grpc.GetJobResultRequest;
import it.jmr.grpc.GetJobResultResponse;
import it.jmr.master.models.JobInfoInternal;

class MapReduceServiceImplTest {

    @Test
    void cancelPendingJobRemovesItFromQueue() {
        final ConcurrentHashMap<String, Path> jars = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Path> jobsPaths = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, JobInfoInternal> jobs = new ConcurrentHashMap<>();
        final Queue<JobInfoInternal> queue = new ConcurrentLinkedDeque<>();
        final MapReduceServiceImpl service = new MapReduceServiceImpl(jars, jobsPaths, jobs, queue);

        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob("job-1");
        jobs.put(jobInfo.getJobId(), jobInfo);
        queue.add(jobInfo);

        final ResponseObserver<CancelJobResponse> observer = new ResponseObserver<>();
        service.cancelJob(CancelJobRequest.newBuilder().setJobId(jobInfo.getJobId()).build(), observer);

        assertTrue(observer.value.getSuccess());
        assertEquals(0, queue.size());
        assertEquals(it.jmr.grpc.JobStatus.CANCELLED, jobInfo.getStatus());
    }

    @Test
    void getJobResultReturnsSerializedPayload() {
        final ConcurrentHashMap<String, Path> jars = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Path> jobsPaths = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, JobInfoInternal> jobs = new ConcurrentHashMap<>();
        final Queue<JobInfoInternal> queue = new ConcurrentLinkedDeque<>();
        final MapReduceServiceImpl service = new MapReduceServiceImpl(jars, jobsPaths, jobs, queue);

        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob("job-1");
        final byte[] result = new byte[] {1, 2, 3, 4};
        jobInfo.setSerializedResult(result);
        jobs.put(jobInfo.getJobId(), jobInfo);

        final ResponseObserver<GetJobResultResponse> observer = new ResponseObserver<>();
        service.getJobResult(GetJobResultRequest.newBuilder().setJobId(jobInfo.getJobId()).build(), observer);

        assertTrue(observer.value.getFound());
        assertTrue(observer.value.getAvailable());
        assertArrayEquals(result, observer.value.getSerializedResult().toByteArray());
    }

    @Test
    void getJobStatusReturnsMapAndReduceProgress() {
        final ConcurrentHashMap<String, Path> jars = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Path> jobsPaths = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, JobInfoInternal> jobs = new ConcurrentHashMap<>();
        final Queue<JobInfoInternal> queue = new ConcurrentLinkedDeque<>();
        final MapReduceServiceImpl service = new MapReduceServiceImpl(jars, jobsPaths, jobs, queue);

        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob("job-1");
        jobInfo.setMapProgress(42);
        jobInfo.setReduceProgress(17);
        jobs.put(jobInfo.getJobId(), jobInfo);

        final ResponseObserver<GetJobStatusResponse> observer = new ResponseObserver<>();
        service.getJobStatus(GetJobStatusRequest.newBuilder().setJobId(jobInfo.getJobId()).build(), observer);

        assertTrue(observer.value.getFound());
        assertEquals(42, observer.value.getJobInfo().getMapProgress());
        assertEquals(17, observer.value.getJobInfo().getReduceProgress());
    }

    private static final class ResponseObserver<T> implements StreamObserver<T> {
        private T value;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            throw new AssertionError("Unexpected error", t);
        }

        @Override
        public void onCompleted() {
        }
    }
}
