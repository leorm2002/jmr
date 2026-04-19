package it.jmr.worker.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import it.jmr.common.utils.JmrUtils;
import it.jmr.common.utils.Pair;

public class FsReduceResultStorage implements ReduceResultStorage {
    private final Path rootDirectory;

    public FsReduceResultStorage(final Path rootDirectory) {
        this.rootDirectory = rootDirectory;
        try {
            Files.createDirectories(rootDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create reduce result storage directory " + rootDirectory, e);
        }
    }

    @Override
    public void saveReducedData(final String jobId, final String taskId, final List<Pair<String, Serializable>> reducedData) {
        final Path resultFile = resolveResultFile(jobId, taskId);
        try {
            Files.createDirectories(resultFile.getParent());
            Files.write(resultFile, JmrUtils.gzip(JmrUtils.serializeObject((Serializable) reducedData)));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to persist reduce result " + jobId + "/" + taskId, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Pair<String, Serializable>> getReducedData(final String jobId, final String taskId) {
        final Path resultFile = resolveResultFile(jobId, taskId);
        if (!Files.exists(resultFile)) {
            return null;
        }

        try {
            return (List<Pair<String, Serializable>>) JmrUtils.deserialize(JmrUtils.gunzip(Files.readAllBytes(resultFile)));
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to read reduce result " + jobId + "/" + taskId, e);
        }
    }

    @Override
    public void deleteTaskResult(final String jobId, final String taskId) {
        final Path resultFile = resolveResultFile(jobId, taskId);
        try {
            Files.deleteIfExists(resultFile);
            final Path jobDirectory = resultFile.getParent();
            if (jobDirectory != null && Files.isDirectory(jobDirectory)) {
                try (var entries = Files.list(jobDirectory)) {
                    if (entries.findAny().isEmpty()) {
                        Files.deleteIfExists(jobDirectory);
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to delete reduce result " + jobId + "/" + taskId, e);
        }
    }

    @Override
    public void clear() {
        JmrUtils.deleteFolder(rootDirectory);
        try {
            Files.createDirectories(rootDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to recreate reduce result storage directory " + rootDirectory, e);
        }
    }

    private Path resolveResultFile(final String jobId, final String taskId) {
        return rootDirectory.resolve(sanitize(jobId)).resolve(sanitize(taskId) + ".bin");
    }

    private static String sanitize(final String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
