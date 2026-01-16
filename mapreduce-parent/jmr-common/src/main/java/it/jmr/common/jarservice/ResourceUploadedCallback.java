package it.jmr.common.jarservice;

public interface ResourceUploadedCallback {
    void onJarUploaded(String jarId, String jarPath);
    void onJobUploaded(String jobId, String jobPath);
}
