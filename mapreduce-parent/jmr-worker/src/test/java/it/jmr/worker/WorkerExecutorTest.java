package it.jmr.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class WorkerExecutorTest {

    @Test
    void computePartitionIdUsesFixedBucketSpace() {
        final int reducePartitionCount = 8;

        assertEquals("bucket-0", WorkerExecutor.computePartitionId("bucket-0", 1));
        final String firstBucket = WorkerExecutor.computePartitionId("alpha", reducePartitionCount);
        final String secondBucket = WorkerExecutor.computePartitionId("alpha", reducePartitionCount);

        assertEquals(firstBucket, secondBucket);
    }
}
