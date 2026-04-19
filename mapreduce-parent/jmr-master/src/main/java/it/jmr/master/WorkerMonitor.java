package it.jmr.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import it.jmr.common.JMRConstants;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.JmrUtils;
import it.jmr.master.models.Worker;

/**
 * Monitors the workers of the cluster if a worker fails, it removes it from the
 * list of available workers
 */
public class WorkerMonitor implements Runnable {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(WorkerMonitor.class);
    private final MasterContext ctx;
    private final Map<String, Integer> consecutiveFailures;

    public WorkerMonitor(MasterContext ctx) {
        this.ctx = ctx;
        this.consecutiveFailures = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        JMRLog.info(LOGGER, "Worker monitor started");
        while (!Thread.currentThread().isInterrupted()) {
            final List<Worker> deadWorkers = new ArrayList<>();
            for (Worker worker : ctx.workers) {
                if (!worker.isAlive()) {
                    final int failureCount = consecutiveFailures.merge(worker.getWorkerId(), 1, Integer::sum);
                    if (failureCount >= JMRConstants.WORKER_HEALTH_FAILURE_THRESHOLD) {
                        deadWorkers.add(worker);
                        JMRLog.error(LOGGER, "Worker {} ({}:{}) not answering for {} consecutive checks, removed from available workers",
                                worker.getWorkerId(), worker.getAddress(), worker.getPort(), failureCount);
                    } else {
                        JMRLog.warn(LOGGER, "Worker {} ({}:{}) missed health check {}/{}", worker.getWorkerId(), worker.getAddress(),
                                worker.getPort(), failureCount, JMRConstants.WORKER_HEALTH_FAILURE_THRESHOLD);
                    }
                } else {
                    consecutiveFailures.remove(worker.getWorkerId());
                }
            }

            ctx.workers.removeAll(deadWorkers);

            // Notify listeners about dead workers
            for (Worker deadWorker : deadWorkers) {
                consecutiveFailures.remove(deadWorker.getWorkerId());
                ctx.notifyWorkerFailed(deadWorker);
            }

            // Sleep for a while before next check
            JmrUtils.sleep(JMRConstants.DEAD_WORKER_MONITOR_SLEEP_MS);
        }
    }

}
