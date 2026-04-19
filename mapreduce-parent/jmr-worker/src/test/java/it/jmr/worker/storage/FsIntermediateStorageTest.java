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

class FsIntermediateStorageTest {

    @TempDir
    Path tempDir;

    @Test
    void persistsAndLoadsPartitionData() throws Exception {
        final FsIntermediateStorage storage = new FsIntermediateStorage(tempDir);
        final List<Serializable> payload = List.of("alpha", "beta", Integer.valueOf(3));

        storage.savePartitionData("task-1", "bucket-2", payload);

        assertTrue(Files.exists(tempDir.resolve("task-1").resolve("bucket-2.bin")));
        assertEquals(payload, storage.getPartitionData("task-1", "bucket-2"));
    }

    @Test
    void clearRemovesPersistedData() throws Exception {
        final FsIntermediateStorage storage = new FsIntermediateStorage(tempDir);

        storage.savePartitionData("task-1", "bucket-2", List.of("value"));
        assertNotNull(storage.getPartitionData("task-1", "bucket-2"));

        storage.clear();

        assertTrue(Files.exists(tempDir));
        assertNull(storage.getPartitionData("task-1", "bucket-2"));
    }

    @Test
    void deleteTaskDataRemovesSingleTaskDirectory() throws Exception {
        final FsIntermediateStorage storage = new FsIntermediateStorage(tempDir);

        storage.savePartitionData("task-1", "bucket-1", List.of("alpha"));
        storage.savePartitionData("task-2", "bucket-2", List.of("beta"));

        storage.deleteTaskData("task-1");

        assertNull(storage.getPartitionData("task-1", "bucket-1"));
        assertEquals(List.of("beta"), storage.getPartitionData("task-2", "bucket-2"));
    }
}
