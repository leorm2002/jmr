package it.jmr.worker.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import it.jmr.common.utils.Pair;

class FsReduceResultStorageTest {

    @TempDir
    Path tempDir;

    @Test
    void persistsAndLoadsReduceData() throws Exception {
        final FsReduceResultStorage storage = new FsReduceResultStorage(tempDir);
        final List<Pair<String, Serializable>> payload = List.of(Pair.of("alpha", Integer.valueOf(1)), Pair.of("beta", Integer.valueOf(2)));

        storage.saveReducedData("job-1", "reduce-2", payload);

        assertTrue(Files.exists(tempDir.resolve("job-1").resolve("reduce-2.bin")));
        assertEquals(payload, storage.getReducedData("job-1", "reduce-2"));
    }

    @Test
    void clearRemovesPersistedReduceData() throws Exception {
        final FsReduceResultStorage storage = new FsReduceResultStorage(tempDir);

        storage.saveReducedData("job-1", "reduce-2", List.of(Pair.of("alpha", Integer.valueOf(1))));
        assertNotNull(storage.getReducedData("job-1", "reduce-2"));

        storage.clear();

        assertTrue(Files.exists(tempDir));
        assertNull(storage.getReducedData("job-1", "reduce-2"));
    }

    @Test
    void deleteTaskResultRemovesOnlySelectedResult() throws Exception {
        final FsReduceResultStorage storage = new FsReduceResultStorage(tempDir);

        storage.saveReducedData("job-1", "reduce-1", List.of(Pair.of("alpha", Integer.valueOf(1))));
        storage.saveReducedData("job-1", "reduce-2", List.of(Pair.of("beta", Integer.valueOf(2))));

        storage.deleteTaskResult("job-1", "reduce-1");

        assertNull(storage.getReducedData("job-1", "reduce-1"));
        assertEquals(List.of(Pair.of("beta", Integer.valueOf(2))), storage.getReducedData("job-1", "reduce-2"));
    }
}
