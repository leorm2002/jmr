package it.jmr.common.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorManager {
    private static final ExecutorService EXECUTOR =Executors.newVirtualThreadPerTaskExecutor();

    private ExecutorManager() {}

    public static ExecutorService getExecutor() {
        return EXECUTOR;
    }

    public static void shutdown() {
        EXECUTOR.shutdownNow();
        System.out.println("🛑 Executor terminato.");
    }
}

