package it.jmr.common.exceptions;

public class JMRException extends Exception {

    public JMRException(String message) {
        super(message);
    }

    public JMRException(String message, Throwable cause) {
        super(message, cause);
    }
}