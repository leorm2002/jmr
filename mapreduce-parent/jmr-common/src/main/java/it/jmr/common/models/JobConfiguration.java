package it.jmr.common.models;

import java.io.Serializable;

import it.jmr.common.providers.DataProviderClient;

public abstract class JobConfiguration<D extends Serializable, V extends Serializable, O extends Serializable> implements Serializable {
    public abstract DataProviderClient<D> getDataProvider();

    public abstract SerializableMapper<D, V> getMapper();

    public abstract SerializableReducer<V, O> getReducer();
}