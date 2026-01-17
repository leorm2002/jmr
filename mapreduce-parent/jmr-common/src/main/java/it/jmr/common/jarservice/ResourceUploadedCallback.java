package it.jmr.common.jarservice;

import java.nio.file.Path;

public interface ResourceUploadedCallback {
    void onJarUploaded(String jarId, Path jarPath);

    void onJobUploaded(String jobId, Path jobPath);
}
