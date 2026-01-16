package it.jmr.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
    private static final ExecutorService EXECUTOR =Executors.newFixedThreadPool(20);

    private ExecutorManager() {}

    public static ExecutorService getExecutor() {
        return EXECUTOR;
    }

    public static void shutdown() {
        EXECUTOR.shutdownNow();
        LOGGER.info("🛑 Executor terminated.");
    }
}

