package it.jmr.common;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.exceptions.JMRException;

public interface IntermediateDataFetcher {
    <VALUE extends Serializable> List<VALUE> fetchIntermediateData(String workerId, String host, int port, String taskId, String partitionId)
            throws JMRException;
}
