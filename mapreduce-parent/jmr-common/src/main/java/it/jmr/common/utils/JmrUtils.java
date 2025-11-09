package it.jmr.common.utils;

public class JmrUtils {
    public static void sleep(int pauseTimeInMillis) {
        try {
            Thread.sleep(pauseTimeInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
