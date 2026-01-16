package it.jmr.client;

import java.io.Serializable;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProvider;
import it.jmr.common.providers.DataProviderClient;

public class Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    // Step 1: Define the DataProvider - no generics on the builder
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

    @SuppressWarnings("unchecked")
    private static class Builder<D extends Serializable, V extends Serializable, O extends Serializable>
            implements DataProviderStep, MapperStep<D>, ReducerStep<D, V>, JobConfiguration<D, V, O> {

        private static final long serialVersionUID = 1L;
        private DataProviderClient<D> dataProviderClient;
        private SerializableMapper<D, V> mapper;
        private SerializableReducer<V, O> reducer;

        @Override
        public <DD extends Serializable> MapperStep<DD> readFrom(DataProvider<DD> dataProvider) {
            Objects.requireNonNull(dataProvider, "DataProvider cannot be null");
            LOGGER.debug("Setting data provider: {}", dataProvider.getClass().getName());
            Builder<DD, ?, ?> builder = (Builder<DD, ?, ?>) this;
            try {
                builder.dataProviderClient = dataProvider.getClient();
            } catch (JMRException e) {
                throw new RuntimeException("Error getting DataProviderClient from DataProvider: " + e.getMessage(), e);
            }
            return (MapperStep<DD>) builder;
        }

        @Override
        public <VV extends Serializable> ReducerStep<D, VV> map(SerializableMapper<D, VV> mapper) {
            Objects.requireNonNull(mapper, "Mapper cannot be null");
            LOGGER.debug("Setting mapper: {}", mapper.getClass().getName());
            Builder<D, VV, ?> builder = (Builder<D, VV, ?>) this;
            builder.mapper = mapper;
            return (ReducerStep<D, VV>) builder;
        }

        @Override
        public <OO extends Serializable> JobConfiguration<D, V, OO> reduce(SerializableReducer<V, OO> reducer) {
            Objects.requireNonNull(reducer, "Reducer cannot be null");
            LOGGER.debug("Setting reducer: {}", reducer.getClass().getName());
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