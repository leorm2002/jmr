package it.jmr.common.providers;

import java.io.Serializable;

import it.jmr.common.exceptions.JMRException;

/**
 * Interface for a server-side DataProvider. Manages data locally and can create
 * clients for remote access.
 */
public interface DataProvider<D extends Serializable> extends Serializable {
    static final long serialVersionUID = 1L;

    /**
     * Initializes the provider and starts the worker server.
     */
    DataProvider<D> init() throws JMRException;

    /**
     * Creates a pre-configured client for remote access to this provider. The
     * client can be serialized and sent to remote workers.
     *
     * @return a DataProviderClient configured to connect to this server.
     */
    DataProviderClient<D> getClient() throws JMRException;

    /**
     * Releases any resources (connections, file handlers, worker server, etc.).
     *
     * @throws JMRException if resource release fails.
     */
    void close() throws JMRException;
}