package it.jmr.client;

import java.io.IOException;
import java.io.Serializable;

import it.jmr.common.models.JobConfiguration;

public class JMRClient {
    private final MapReduceClient mapReduceClient;
    private String jobIdentifier;

    public JMRClient(String host, int port) {
        this.mapReduceClient = new MapReduceClient(host, port);
    }

    public <D extends Serializable, V extends Serializable, O extends Serializable> void submit(String jarPath, JobConfiguration<D, V, O> jobConfig) {

        // Invia il jar e la configurazione al cluster MapReduce
        final String jarId;
        try {
            jarId = mapReduceClient.uploadJar(jarPath);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to upload jar", e);
        }

        // Invia il job per l'esecuzione
        final String jobId;
        try {
            jobId = mapReduceClient.uploadJob(jobConfig);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to upload job", e);
        }

        this.jobIdentifier = mapReduceClient.submitJob(jarId, jobId);

    }

    public void getJobStatus() {
        if (this.jobIdentifier == null) {
            throw new IllegalStateException("No job has been submitted yet.");
        }
        mapReduceClient.getJobStatus(this.jobIdentifier);
    }
}
