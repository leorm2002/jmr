package it.jmr.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.JobStatus;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

class MasterExecutorTest {

    @Test
    void chunkIntoMapTasksHandlesSmallDatasetsWithoutLooping() throws Exception {
        final Method chunkIntoMapTasks = MasterExecutor.class.getDeclaredMethod("chunkIntoMapTasks", List.class, String.class, String.class,
                long.class);
        chunkIntoMapTasks.setAccessible(true);

        final List<Worker> workers = List.of(new Worker(new WorkerI("worker-1", "localhost", 50051)));

        @SuppressWarnings("unchecked")
        final List<MasterExecutor.UnassignedMapTasks> tasks = assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> (List<MasterExecutor.UnassignedMapTasks>) chunkIntoMapTasks.invoke(null, workers, "jar-1", "job-1", 1L));

        assertEquals(1, tasks.size());
        assertEquals(Pair.of(0L, 1L), tasks.getFirst().chunks());
    }

    @Test
    void chunkIntoMapTasksRejectsEmptyWorkerList() throws Exception {
        final Method chunkIntoMapTasks = MasterExecutor.class.getDeclaredMethod("chunkIntoMapTasks", List.class, String.class, String.class,
                long.class);
        chunkIntoMapTasks.setAccessible(true);

        final InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                () -> chunkIntoMapTasks.invoke(null, List.of(), "jar-1", "job-1", 10L));

        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }

    @Test
    void jobExecutionContextDeduplicatesQueuedReduceTasks() {
        final MasterExecutor.JobExecutionContext<String> ctx = new MasterExecutor.JobExecutionContext<>();
        final MasterExecutor.UnassignedReduceTasks task = new MasterExecutor.UnassignedReduceTasks("job-1", "reduce-1", "jar-1", "partition-1",
                List.of());

        ctx.enqueueReduceTask(task);
        ctx.enqueueReduceTask(task);

        assertEquals(1, ctx.unassignedReduceQueue.size());
        assertNotNull(ctx.pollReduceTask());
        assertEquals(0, ctx.unassignedReduceQueue.size());
    }

    @Test
    void chunkIntoMapTasksLeavesRoomForMultipleReduceBuckets() throws Exception {
        final Method chunkIntoMapTasks = MasterExecutor.class.getDeclaredMethod("chunkIntoMapTasks", List.class, String.class, String.class,
                long.class);
        chunkIntoMapTasks.setAccessible(true);

        final List<Worker> workers = List.of(new Worker(new WorkerI("worker-1", "localhost", 50051)),
                new Worker(new WorkerI("worker-2", "localhost", 50052)));

        @SuppressWarnings("unchecked")
        final List<MasterExecutor.UnassignedMapTasks> tasks = (List<MasterExecutor.UnassignedMapTasks>) chunkIntoMapTasks.invoke(null, workers,
                "jar-1", "job-1", 100L);

        assertTrue(tasks.size() > 1);
    }

    @Test
    void executeMappingFailsFastWhenPhaseHasAlreadyFailed() throws Exception {
        final Method executeMapping = MasterExecutor.class.getDeclaredMethod("executeMapping", JobInfoInternal.class, AtomicBoolean.class,
                java.util.concurrent.Future.class, java.util.concurrent.Future.class);
        executeMapping.setAccessible(true);

        final JobInfoInternal jobInfo = JobInfoInternal.recievedJob("job-1");
        jobInfo.setStatus(JobStatus.FAILED);
        jobInfo.setErrorMessage("boom");
        final AtomicBoolean stopForError = new AtomicBoolean(true);

        final InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                () -> executeMapping.invoke(null, jobInfo, stopForError, CompletableFuture.completedFuture(null),
                        CompletableFuture.completedFuture(null)));

        assertInstanceOf(JMRException.class, exception.getCause());
    }
}
