package it.jmr.master;

import java.util.ArrayList;
import java.util.List;

import it.jmr.client.JMRClient;
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

    public WorkerMonitor(MasterContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void run() {
        JMRLog.info(LOGGER, "Worker monitor started");
        while (!Thread.currentThread().isInterrupted()) {
            final List<Worker> deadWorkers = new ArrayList<>();
            for (Worker worker : ctx.workers) {
                if (!worker.isAlive()) {
                    deadWorkers.add(worker);
                    JMRLog.error(LOGGER, "Worker {} ({}:{}) not answering, removed from available workers", worker.getWorkerId(), worker.getAddress(),
                            worker.getPort());
                }
            }

            ctx.workers.removeAll(deadWorkers);

            // Sleep for a while before next check
            JmrUtils.sleep(JMRConstants.DEAD_WORKER_MONITOR_SLEEP_MS);
        }
    }

}
