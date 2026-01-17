package it.jmr.client;

import java.io.Serializable;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;
import it.jmr.common.models.JobConfiguration;

public class JMRClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JMRClient.class);
    private final MapReduceClient mapReduceClient;
    private String jobIdentifier;

    public JMRClient(String host, int port) {
        this.mapReduceClient = new MapReduceClient(host, port);
    }

    public <D extends Serializable, V extends Serializable, O extends Serializable> void submit(Path jarPath, JobConfiguration<D, V, O> jobConfig)
            throws JMRException {
        LOGGER.info("Submitting job with jar path: {}", jarPath);
        final String jarId;
        try {
            jarId = mapReduceClient.uploadJar(jarPath);
            LOGGER.debug("Uploaded jar with id: {}", jarId);
        } catch (JMRException e) {
            LOGGER.error("Failed to upload jar", e);
            throw new JMRException("Failed to upload jar", e);
        }

        final String jobId;
        try {
            jobId = mapReduceClient.uploadJob(jobConfig);
            LOGGER.debug("Uploaded job with id: {}", jobId);
        } catch (JMRException e) {
            LOGGER.error("Failed to upload job", e);
            throw new JMRException("Failed to upload job", e);
        }

        this.jobIdentifier = mapReduceClient.submitJob(jarId, jobId);
        LOGGER.info("Submitted job with identifier: {}", this.jobIdentifier);

    }

    public String getJobStatus() {
        if (this.jobIdentifier == null) {
            throw new IllegalStateException("No job has been submitted yet.");
        }
        LOGGER.info("Getting status for job: {}", this.jobIdentifier);
        final String status = mapReduceClient.getJobStatus(this.jobIdentifier);
        return status;
    }

    @Override
    public void close() throws Exception {
        mapReduceClient.shutdown();
    }
}
