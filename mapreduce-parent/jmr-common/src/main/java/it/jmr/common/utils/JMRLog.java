package it.jmr.common.utils;

import java.util.Arrays;
import java.util.Objects;

import org.slf4j.Logger;

/**
 * Utility class for logging with SLF4J
 */
public class JMRLog {

    public static void info(Logger logger, String message, Object... args) {
        logger.info(message, getPrintableArguments(args));
    }

    public static void error(Logger logger, String message, Object... args) {
        logger.error(message, getPrintableArguments(args));
    }

    public static void debug(Logger logger, String message, Object... args) {
        logger.debug(message, getPrintableArguments(args));
    }

    public static void warn(Logger logger, String message, Object... args) {
        logger.warn(message, getPrintableArguments(args));
    }

    public static void trace(Logger logger, String message, Object... args) {
        logger.trace(message, getPrintableArguments(args));
    }

    private static Object[] getPrintableArguments(Object[] arguments) {
        return Arrays.stream(arguments).filter(Objects::nonNull).map(Object::toString).toArray();
    }

}
