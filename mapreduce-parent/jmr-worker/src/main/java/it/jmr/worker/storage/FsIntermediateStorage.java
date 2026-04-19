package it.jmr.worker.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import it.jmr.common.PartitionInfo;
import it.jmr.common.utils.JmrUtils;

public class FsIntermediateStorage implements IntermediateStorage {
    private final Path rootDirectory;

    public FsIntermediateStorage(final Path rootDirectory) {
        this.rootDirectory = rootDirectory;
        try {
            Files.createDirectories(rootDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create intermediate storage directory " + rootDirectory, e);
        }
    }

    @Override
    public PartitionInfo savePartitionData(final String taskId, final String partitionId, final List<Serializable> data) {
        final Path partitionFile = resolvePartitionFile(taskId, partitionId);
        try {
            Files.createDirectories(partitionFile.getParent());
            Files.write(partitionFile, JmrUtils.gzip(JmrUtils.serializeObject((Serializable) data)));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to persist intermediate partition " + taskId + "/" + partitionId, e);
        }
        return new PartitionInfo(taskId, partitionId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Serializable> getPartitionData(final String taskId, final String partitionId) {
        final Path partitionFile = resolvePartitionFile(taskId, partitionId);
        if (!Files.exists(partitionFile)) {
            return null;
        }

        try {
            return (List<Serializable>) JmrUtils.deserialize(JmrUtils.gunzip(Files.readAllBytes(partitionFile)));
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to read intermediate partition " + taskId + "/" + partitionId, e);
        }
    }

    @Override
    public void deleteTaskData(final String taskId) {
        JmrUtils.deleteFolder(rootDirectory.resolve(sanitize(taskId)));
    }

    @Override
    public void clear() {
        JmrUtils.deleteFolder(rootDirectory);
        try {
            Files.createDirectories(rootDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to recreate intermediate storage directory " + rootDirectory, e);
        }
    }

    private Path resolvePartitionFile(final String taskId, final String partitionId) {
        return rootDirectory.resolve(sanitize(taskId)).resolve(sanitize(partitionId) + ".bin");
    }

    private static String sanitize(final String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
