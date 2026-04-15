package it.jmr.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import it.jmr.common.utils.Pair;
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
}
