package com.example;

import java.util.List;

import it.jmr.common.providers.DataProviderClient;

final class DataProviderClientImplementation implements DataProviderClient<String> {
    @Override
    public void init() {
    }

    @Override
    public long size() {
        return 6;
    }

    @Override
    public List<String> fetchChunk(long offset, long limit) {
        return List.of("Alice", "Alice", "Alice", "Bob", "Bob", "Charlie");
    }

    @Override
    public void close() {
    }
}