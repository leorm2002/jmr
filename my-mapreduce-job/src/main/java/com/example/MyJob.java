package com.example;

import java.util.List;

import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.Pair;

public class MyJob implements JobConfiguration<String, Integer, Integer> {

    @Override
    public DataProviderClient<String> getDataProvider() {
        return new DataProviderClientImplementation();
    }

    @Override
    public SerializableMapper<String, Integer> getMapper() {
        return a -> List.of(Pair.of(a, 1));
    }

    @Override
    public SerializableReducer<Integer, Integer> getReducer() {
        return (key, values) -> {
            final int sum = values.stream().mapToInt(Integer::intValue).sum();
            return Pair.of(key, sum);
        };
    }

}
