package it.jmr.common.providers;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.exceptions.JMRException;

/**
 * Interface for a remote client that accesses a DataProvider. Implementations
 * of this interface are serializable and can be sent from the master to remote
 * workers to access distributed data.
 */
public interface DataProviderClient<D extends Serializable> extends Serializable, AutoCloseable {
    static final long serialVersionUID = 1L;

    /**
     * Initializes the connection to the remote server. Must be called before any
     * fetch operation.
     */
    void init();

    /**
     * Returns the total size of the data from the remote server.
     *
     * @return total number of available items.
     * @throws JMRException if connection or access problems occur.
     */
    long size() throws JMRException;

    /**
     * Retrieves a block of data from the remote server.
     *
     * @param offset starting position (0-based).
     * @param limit  maximum number of items to read.
     * @return list of retrieved items (can be empty if offset >= size).
     * @throws JMRException if connection or access problems occur.
     */
    List<D> fetchChunk(long offset, long limit) throws JMRException;

    /**
     * Closes the connection to the remote server.
     *
     * @throws JMRException if closing fails.
     */
    @Override
    void close() throws JMRException;
}