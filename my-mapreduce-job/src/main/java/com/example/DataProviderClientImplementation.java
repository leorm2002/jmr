package com.example;

import java.io.IOException;
import java.util.List;

import it.jmr.common.providers.DataProviderClient;

final class DataProviderClientImplementation implements DataProviderClient<String> {
    @Override
    public void init() {
    }

    @Override
    public long size() throws IOException {
        return 6;
    }

    @Override
    public List<String> fetchChunk(long offset, long limit) throws IOException {
        return List.of("Alice", "Alice", "Alice", "Bob", "Bob", "Charlie");
    }

    @Override
    public void close() throws IOException {
    }
}