package it.jmr.common;

import java.security.PublicKey;

public class JMRConstants {
    private static final int KMS = 1000;
    public static final String HEALTH_SERVICE_NAME = "";
    public static final int REQUEST_TIMEOUT_S = 5;
    public static final int UPLOAD_CHUNK_SIZE = 4096; // 4KB
    public static final int MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024; // 1GB
    public static final int JAR_UPLOAD_TIMEOUT_M = 5; // 5 minutes
    public static final int JOB_UPLOAD_TIMEOUT_M = 5; // 5 minutes
    public static final int MAP_TASK_MONITOR_SLEEP_MS = 2 * KMS; // 2 seconds
    public static final int MAP_TASK_SCHEDULER_SLEEP_MS = 2 * KMS; // 2 seconds
    public static final int DEAD_WORKER_MONITOR_SLEEP_MS = 5 * KMS; // 5 seconds
    public static final int MASTER_POLL_INTERVAL_MS = 2 * KMS; // 2 seconds
    
}
