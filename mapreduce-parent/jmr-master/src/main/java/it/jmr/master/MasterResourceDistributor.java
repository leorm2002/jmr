package it.jmr.master;

import it.jmr.common.jarservice.JarServiceClient;
import it.jmr.common.jarservice.JobServiceClient;
import it.jmr.common.jarservice.ResourceUploadedCallback;
import it.jmr.master.models.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterResourceDistributor implements ResourceUploadedCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterResourceDistributor.class);

    private final MasterContext masterContext;

    public MasterResourceDistributor(MasterContext masterContext) {
        this.masterContext = masterContext;
    }

    @Override
    public void onJarUploaded(String jarId, String jarPath) {
        LOGGER.info("Master received new JAR: {} at {}", jarId, jarPath);
        for (Worker worker : masterContext.workers) {
            LOGGER.info("Pushing JAR {} to worker {}", jarId, worker.getWorkerId());
            try {
                JarServiceClient.uploadJar(jarPath, jarId, worker.getJarAsyncStub());
            } catch (Exception e) {
                LOGGER.error("Error uploading JAR {} to worker {}: {}", jarId, worker.getWorkerId(), e.getMessage());
            }
        }
    }

    @Override
    public void onJobUploaded(String jobId, String jobPath) {
        LOGGER.info("Master received new JOB: {} at {}", jobId, jobPath);
        for (Worker worker : masterContext.workers) {
            LOGGER.info("Pushing JOB {} to worker {}", jobId, worker.getWorkerId());
            try {
                // Forward the job file directly without deserializing
                JobServiceClient.uploadJobFromFile(jobPath, jobId, worker.getJobAsyncStub());
            } catch (Exception e) {
                LOGGER.error("Error uploading job {} to worker {}: {}", jobId, worker.getWorkerId(), e.getMessage());
            }
        }
    }
}
