package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableSearch;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealCompactionPathFoundationTest extends RealCompactionPathIntegrationBaseTest {

    @Test
    void realPathFlushCompactionAndSearchSmokeTest() throws Exception {
        flushSingleEntryToL0("real-path-key", "real-path-value", 1L);
        assertTrue(harness.getSSTableCount(0) >= 1, "Flush should create an L0 SSTable");

        CompactionManager manager = harness.createCompactionManager();
        pollCompactionWithBoundedChecks(manager, 20);
        manager.shutdown();

        byte[] actual = new SSTableSearch().search(
                new ByteArrayWrapper("real-path-key".getBytes(StandardCharsets.UTF_8)));
        assertArrayEquals("real-path-value".getBytes(StandardCharsets.UTF_8), actual,
                "Real compaction-path read should return inserted value");
    }
}

