package it.jmr.master.events;

import it.jmr.master.models.Worker;

/**
 * Listener interface for worker failure events.
 * Implementations can react to worker failures by rescheduling tasks
 * or invalidating intermediate data.
 */
@FunctionalInterface
public interface WorkerFailureListener {
    void onWorkerFailed(Worker worker);
}
