package it.jmr.common.models;

import java.io.Serializable;

import it.jmr.common.providers.DataProviderClient;

public interface JobConfiguration<D extends Serializable, V extends Serializable, O extends Serializable> extends Serializable {
    static final long serialVersionUID = 1L;

    public DataProviderClient<D> getDataProvider();

    public SerializableMapper<D, V> getMapper();

    public SerializableReducer<V, O> getReducer();
}