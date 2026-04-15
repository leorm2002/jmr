package com.example;

import java.util.List;

import it.jmr.common.providers.DataProviderClient;

final class DataProviderClientImplementation implements DataProviderClient<String> {
    private static final List<String> DATA = List.of("Alice", "Alice", "Alice", "Bob", "Bob", "Charlie");

    @Override
    public void init() {
    }

    @Override
    public long size() {
        return DATA.size();
    }

    @Override
    public List<String> fetchChunk(long offset, long limit) {
        if (offset < 0 || limit <= 0 || offset >= DATA.size()) {
            return List.of();
        }

        final int startIndex = (int) offset;
        final int endIndex = (int) Math.min(offset + limit, DATA.size());
        return DATA.subList(startIndex, endIndex);
    }

    @Override
    public void close() {
    }
}
