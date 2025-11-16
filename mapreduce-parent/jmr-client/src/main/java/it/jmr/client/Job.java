package it.jmr.client;

import java.io.Serializable;
import java.util.Objects;

import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProvider;
import it.jmr.common.providers.DataProviderClient;

public class Job {

    // Step 1: Definire il DataProvider - nessun generic sul builder
    public static DataProviderStep builder() {
        return new Builder<>();
    }

    public interface DataProviderStep {
        <D extends Serializable> MapperStep<D> readFrom(DataProvider<D> dataProvider);
    }

    public interface MapperStep<D extends Serializable> {
        <V extends Serializable> ReducerStep<D, V> map(SerializableMapper<D, V> mapper);
    }

    public interface ReducerStep<D extends Serializable, V extends Serializable> {
        <O extends Serializable> JobConfiguration<D, V, O> reduce(SerializableReducer<V, O> reducer);
    }

    private static class Builder<D extends Serializable, V extends Serializable, O extends Serializable>
            implements DataProviderStep, MapperStep<D>, ReducerStep<D, V>, JobConfiguration<D, V, O> {

        private static final long serialVersionUID = 1L;
        private transient DataProvider<D> dataProvider;
        private DataProviderClient<D> dataProviderClient;
        private SerializableMapper<D, V> mapper;
        private SerializableReducer<V, O> reducer;

        @Override
        public <DD extends Serializable> MapperStep<DD> readFrom(DataProvider<DD> dataProvider) {
            Objects.requireNonNull(dataProvider, "DataProvider cannot be null");
            Builder<DD, ?, ?> builder = (Builder<DD, ?, ?>) this;
            builder.dataProvider = dataProvider;
            builder.dataProviderClient = dataProvider.getClient();
            return (MapperStep<DD>) builder;
        }

        @Override
        public <VV extends Serializable> ReducerStep<D, VV> map(SerializableMapper<D, VV> mapper) {
            Objects.requireNonNull(mapper, "Mapper cannot be null");
            Builder<D, VV, ?> builder = (Builder<D, VV, ?>) this;
            builder.mapper = mapper;
            return (ReducerStep<D, VV>) builder;
        }

        @Override
        public <OO extends Serializable> JobConfiguration<D, V, OO> reduce(SerializableReducer<V, OO> reducer) {
            Objects.requireNonNull(reducer, "Reducer cannot be null");
            Builder<D, V, OO> builder = (Builder<D, V, OO>) this;
            builder.reducer = reducer;
            return (JobConfiguration<D, V, OO>) builder;
        }

        @Override
        public DataProviderClient<D> getDataProvider() {
            return dataProviderClient;
        }

        @Override
        public SerializableMapper<D, V> getMapper() {
            return mapper;
        }

        @Override
        public SerializableReducer<V, O> getReducer() {
            return reducer;
        }

    }
}