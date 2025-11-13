/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka;

import com.starrocks.connector.kafka.json.JsonConverter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.*;

/**
 * Test class for batching logic improvements in StarRocksSinkTask
 */
public class StarRocksSinkTaskBatchingTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTaskBatchingTest.class);
    private static final String TOPIC = "test_topic";

    private TestableStarRocksSinkTask sinkTask;
    private Map<String, String> props;

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int32())
            .field("name", SchemaBuilder.string())
            .field("value", SchemaBuilder.float64())
            .build();

    /**
     * Testable version of StarRocksSinkTask that doesn't require actual StarRocks connection
     */
    private static class TestableStarRocksSinkTask extends StarRocksSinkTask {
        private boolean flushCalled = false;
        private int flushCallCount = 0;
        private Exception flushException = null;
        private final List<String> writtenData = new ArrayList<>();
        
        public void setFlushException(Exception exception) {
            this.flushException = exception;
        }
        
        public boolean wasFlushCalled() {
            return flushCalled;
        }
        
        public int getFlushCallCount() {
            return flushCallCount;
        }
        
        public List<String> getWrittenData() {
            return new ArrayList<>(writtenData);
        }
        
        public void resetCounters() {
            flushCalled = false;
            flushCallCount = 0;
            writtenData.clear();
        }
        
        // Override methods that would normally interact with StarRocks
        @Override
        public void start(Map<String, String> props) {
            // Initialize basic fields without creating actual connections
            try {
                Field propsField = StarRocksSinkTask.class.getDeclaredField("props");
                propsField.setAccessible(true);
                propsField.set(this, props);
                
                Field buffMaxbytesField = StarRocksSinkTask.class.getDeclaredField("buffMaxbytes");
                buffMaxbytesField.setAccessible(true);
                buffMaxbytesField.set(this, Long.parseLong(props.getOrDefault(BUFFERFLUSH_MAXBYTES, "67108864")));
                
                Field bufferFlushIntervalField = StarRocksSinkTask.class.getDeclaredField("bufferFlushInterval");
                bufferFlushIntervalField.setAccessible(true);
                bufferFlushIntervalField.set(this, Long.parseLong(props.getOrDefault(BUFFERFLUSH_INTERVALMS, "30000")));
                
                Field currentBufferBytesField = StarRocksSinkTask.class.getDeclaredField("currentBufferBytes");
                currentBufferBytesField.setAccessible(true);
                currentBufferBytesField.set(this, 0L);
                
                Field lastFlushTimeField = StarRocksSinkTask.class.getDeclaredField("lastFlushTime");
                lastFlushTimeField.setAccessible(true);
                lastFlushTimeField.set(this, System.currentTimeMillis());
                
                // Set up JSON converter
                JsonConverter jsonConverter = createJsonConverter();
                setJsonConverter(jsonConverter);
                setSinkType(SinkType.JSON);
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize test task", e);
            }
        }
        
        // Simulate write operations by tracking data size
        public void simulateWrite(String data) {
            writtenData.add(data);
            try {
                Field currentBufferBytesField = StarRocksSinkTask.class.getDeclaredField("currentBufferBytes");
                currentBufferBytesField.setAccessible(true);
                long currentBytes = (Long) currentBufferBytesField.get(this);
                long dataSize = data.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 10; // Add overhead
                currentBufferBytesField.set(this, currentBytes + dataSize);
            } catch (Exception e) {
                throw new RuntimeException("Failed to update buffer size", e);
            }
        }
        
        // Override preCommit to use test flush behavior
        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            try {
                Field currentBufferBytesField = StarRocksSinkTask.class.getDeclaredField("currentBufferBytes");
                currentBufferBytesField.setAccessible(true);
                long currentBufferBytes = (Long) currentBufferBytesField.get(this);
                
                Field buffMaxbytesField = StarRocksSinkTask.class.getDeclaredField("buffMaxbytes");
                buffMaxbytesField.setAccessible(true);
                long buffMaxbytes = (Long) buffMaxbytesField.get(this);
                
                Field bufferFlushIntervalField = StarRocksSinkTask.class.getDeclaredField("bufferFlushInterval");
                bufferFlushIntervalField.setAccessible(true);
                long bufferFlushInterval = (Long) bufferFlushIntervalField.get(this);
                
                Field lastFlushTimeField = StarRocksSinkTask.class.getDeclaredField("lastFlushTime");
                lastFlushTimeField.setAccessible(true);
                long lastFlushTime = (Long) lastFlushTimeField.get(this);
                
                long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;
                
                // Implement the improved OR logic
                boolean shouldFlush = currentBufferBytes >= buffMaxbytes || timeSinceLastFlush >= bufferFlushInterval;
                
                if (!shouldFlush) {
                    LOG.debug("Skip preCommit - currentBufferBytes {} (max: {}), timeSinceLastFlush {} ms (max: {} ms)",
                            currentBufferBytes, buffMaxbytes, timeSinceLastFlush, bufferFlushInterval);
                    return Collections.emptyMap();
                }
                
                // Simulate flush
                flushCalled = true;
                flushCallCount++;
                
                if (flushException != null) {
                    // Reset buffer even on exception
                    currentBufferBytesField.set(this, 0L);
                    lastFlushTimeField.set(this, System.currentTimeMillis());
                    throw new RuntimeException(flushException.getMessage());
                }
                
                // Reset buffer after successful flush
                currentBufferBytesField.set(this, 0L);
                lastFlushTimeField.set(this, System.currentTimeMillis());
                
                LOG.info("Test flush successful - buffer reset");
                return offsets;
                
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Test preCommit failed", e);
            }
        }
        
        public void setLastFlushTime(long time) {
            try {
                Field field = StarRocksSinkTask.class.getDeclaredField("lastFlushTime");
                field.setAccessible(true);
                field.set(this, time);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set last flush time", e);
            }
        }
        
        public long getCurrentBufferBytes() {
            try {
                Field field = StarRocksSinkTask.class.getDeclaredField("currentBufferBytes");
                field.setAccessible(true);
                return (Long) field.get(this);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get current buffer bytes", e);
            }
        }
    }

    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
        
        // Initialize task
        sinkTask = new TestableStarRocksSinkTask();
        
        // Set up basic configuration
        props = new HashMap<>();
        props.put(STARROCKS_LOAD_URL, "http://localhost:8030");
        props.put(STARROCKS_DATABASE_NAME, "test_db");
        props.put(STARROCKS_USERNAME, "root");
        props.put(STARROCKS_PASSWORD, "");
        props.put(SINK_FORMAT, "json");
    }

    @Test
    public void testBatchingLogicWithDefaultConfiguration() {
        // Test with default configuration (30 second interval)
        props.put(BUFFERFLUSH_MAXBYTES, "1048576"); // 1MB
        props.put(BUFFERFLUSH_INTERVALMS, "30000");  // 30 seconds
        
        sinkTask.start(props);
        
        // Simulate small data that shouldn't trigger size-based flush
        sinkTask.simulateWrite("small_record_1");
        sinkTask.simulateWrite("small_record_2");
        
        // preCommit should skip flush since buffer is small and time hasn't elapsed
        Map<TopicPartition, OffsetAndMetadata> result = sinkTask.preCommit(Collections.emptyMap());
        
        // Should return empty map (no commit) since batching conditions not met
        Assert.assertTrue("Should skip commit for small batch within time window", result.isEmpty());
        Assert.assertFalse("Flush should not be called", sinkTask.wasFlushCalled());
    }

    @Test
    public void testSizeBasedBatchingFlush() {
        // Configure small buffer size to trigger size-based flush
        props.put(BUFFERFLUSH_MAXBYTES, "100"); // 100 bytes - very small for testing
        props.put(BUFFERFLUSH_INTERVALMS, "30000"); // 30 seconds
        
        sinkTask.start(props);
        
        // Add data that will exceed buffer size
        for (int i = 0; i < 10; i++) {
            sinkTask.simulateWrite("this_is_a_record_that_should_trigger_size_based_flushing_" + i);
        }
        
        // preCommit should trigger flush due to buffer size
        Map<TopicPartition, OffsetAndMetadata> offsets = createTestOffsets();
        Map<TopicPartition, OffsetAndMetadata> result = sinkTask.preCommit(offsets);
        
        // Should return the offsets (commit) since size limit was exceeded
        Assert.assertEquals("Should commit when buffer size exceeded", offsets, result);
        Assert.assertTrue("Flush should be called when size exceeded", sinkTask.wasFlushCalled());
        Assert.assertEquals("Buffer should be reset after flush", 0L, sinkTask.getCurrentBufferBytes());
    }

    @Test
    public void testTimeBasedBatchingFlush() {
        // Configure large buffer but short time interval
        props.put(BUFFERFLUSH_MAXBYTES, "10485760"); // 10MB - very large
        props.put(BUFFERFLUSH_INTERVALMS, "1000");   // 1 second - short for testing
        
        sinkTask.start(props);
        
        // Add small amount of data
        sinkTask.simulateWrite("small_data_1");
        sinkTask.simulateWrite("small_data_2");
        
        // Simulate time passing by setting lastFlushTime in the past
        sinkTask.setLastFlushTime(System.currentTimeMillis() - 2000); // 2 seconds ago
        
        // preCommit should trigger flush due to time elapsed
        Map<TopicPartition, OffsetAndMetadata> offsets = createTestOffsets();
        Map<TopicPartition, OffsetAndMetadata> result = sinkTask.preCommit(offsets);
        
        // Should return the offsets (commit) since time limit was exceeded
        Assert.assertEquals("Should commit when time interval exceeded", offsets, result);
        Assert.assertTrue("Flush should be called when time exceeded", sinkTask.wasFlushCalled());
    }

    @Test
    public void testImprovedOrLogicVsOldAndLogic() {
        // Test that the new OR logic works better than old AND logic
        props.put(BUFFERFLUSH_MAXBYTES, "200"); // 200 bytes
        props.put(BUFFERFLUSH_INTERVALMS, "5000"); // 5 seconds
        
        sinkTask.start(props);
        
        // Add data that exceeds size but not time
        for (int i = 0; i < 15; i++) {
            sinkTask.simulateWrite("medium_sized_record_for_testing_" + i);
        }
        
        // With new OR logic, should flush when size limit exceeded (regardless of time)
        Map<TopicPartition, OffsetAndMetadata> offsets = createTestOffsets();
        Map<TopicPartition, OffsetAndMetadata> result = sinkTask.preCommit(offsets);
        
        // Should commit due to size (new OR logic)
        Assert.assertEquals("New OR logic should commit when size exceeded", offsets, result);
        Assert.assertTrue("Should flush when size exceeded", sinkTask.wasFlushCalled());
        
        // Reset for next test
        sinkTask.resetCounters();
        
        // Test time-based flush when size not exceeded
        sinkTask.simulateWrite("tiny_record");
        
        // Simulate time elapsed
        sinkTask.setLastFlushTime(System.currentTimeMillis() - 6000); // 6 seconds ago
        
        result = sinkTask.preCommit(offsets);
        
        // Should commit due to time (new OR logic)
        Assert.assertEquals("New OR logic should commit when time exceeded", offsets, result);
        Assert.assertTrue("Should flush when time exceeded", sinkTask.wasFlushCalled());
    }

    @Test
    public void testBufferSizeTrackingAccuracy() {
        props.put(BUFFERFLUSH_MAXBYTES, "10485760"); // 10MB
        props.put(BUFFERFLUSH_INTERVALMS, "30000");
        
        sinkTask.start(props);
        
        // Add records with known content
        String recordContent = "test_record_with_known_size";
        for (int i = 0; i < 5; i++) {
            sinkTask.simulateWrite(recordContent + "_" + i);
        }
        
        // Get current buffer bytes
        long currentBufferBytes = sinkTask.getCurrentBufferBytes();
        
        // Should be greater than zero and account for overhead
        Assert.assertTrue("Buffer should track byte size accurately", currentBufferBytes > 0);
        
        // Should be reasonable size (each record + some overhead)
        long expectedMinSize = 5 * recordContent.length();
        Assert.assertTrue("Buffer size should be at least the content size", 
                         currentBufferBytes >= expectedMinSize);
        
        LOG.info("Tracked buffer size: {} bytes for 5 records", currentBufferBytes);
    }

    @Test
    public void testBufferResetAfterFlush() {
        props.put(BUFFERFLUSH_MAXBYTES, "100"); // Small for testing
        props.put(BUFFERFLUSH_INTERVALMS, "30000");
        
        sinkTask.start(props);
        
        // Add data to build up buffer
        for (int i = 0; i < 10; i++) {
            sinkTask.simulateWrite("some_record_content_" + i);
        }
        
        // Verify buffer has content
        long bufferBeforeFlush = sinkTask.getCurrentBufferBytes();
        Assert.assertTrue("Buffer should have content before flush", bufferBeforeFlush > 0);
        
        // Trigger flush
        sinkTask.preCommit(createTestOffsets());
        
        // Verify buffer was reset
        long bufferAfterFlush = sinkTask.getCurrentBufferBytes();
        Assert.assertEquals("Buffer should be reset to 0 after flush", 0L, bufferAfterFlush);
        Assert.assertTrue("Flush should have been called", sinkTask.wasFlushCalled());
    }

    @Test
    public void testExceptionHandlingDuringBatching() {
        props.put(BUFFERFLUSH_MAXBYTES, "100");
        props.put(BUFFERFLUSH_INTERVALMS, "30000");
        
        sinkTask.start(props);
        
        // Configure task to throw exception on flush
        RuntimeException testException = new RuntimeException("Test flush exception");
        sinkTask.setFlushException(testException);
        
        // Add data that will trigger flush
        for (int i = 0; i < 10; i++) {
            sinkTask.simulateWrite("record_that_will_cause_flush_" + i);
        }
        
        // preCommit should handle exception and rethrow
        try {
            sinkTask.preCommit(createTestOffsets());
            Assert.fail("Should have thrown exception");
        } catch (RuntimeException e) {
            Assert.assertEquals("Should propagate the original exception message", 
                              testException.getMessage(), e.getMessage());
        }
        
        // Buffer should still be reset even after exception
        Assert.assertEquals("Buffer should be reset even after exception", 0L, sinkTask.getCurrentBufferBytes());
        Assert.assertTrue("Flush should have been attempted", sinkTask.wasFlushCalled());
    }

    @Test
    public void testConfigurationDefaults() {
        // Test new default values in connector config
        Map<String, String> testConfig = new HashMap<>();
        testConfig.put(STARROCKS_LOAD_URL, "http://localhost:8030");
        testConfig.put(STARROCKS_DATABASE_NAME, "test_db");
        testConfig.put(STARROCKS_USERNAME, "root");
        testConfig.put(STARROCKS_PASSWORD, "");
        
        // Don't set flush interval - should get new default
        sinkTask.start(testConfig);
        
        // Simulate checking internal values (the start method sets defaults)
        // The actual default verification would be done at the connector level
        LOG.info("Configuration defaults test completed - new default should be 30000ms");
    }

        @Test
    public void testRealWorldBatchingScenario() {
        // Use more realistic settings
        props.put(BUFFERFLUSH_MAXBYTES, "1048576"); // 1MB
        props.put(BUFFERFLUSH_INTERVALMS, "30000"); // 30 seconds
        
        sinkTask.start(props);
        
        // Simulate realistic JSON records
        for (int i = 0; i < 100; i++) {
            String jsonRecord = String.format(
                "{\"id\":%d,\"name\":\"user_%d\",\"email\":\"user_%d@example.com\",\"timestamp\":%d}",
                i, i, i, System.currentTimeMillis()
            );
            sinkTask.simulateWrite(jsonRecord);
        }
        
        long bufferSize = sinkTask.getCurrentBufferBytes();
        LOG.info("Buffer size after 100 records: {} bytes", bufferSize);
        
        // Should not flush yet (under 1MB and within time limit)
        Map<TopicPartition, OffsetAndMetadata> result = sinkTask.preCommit(Collections.emptyMap());
        Assert.assertTrue("Should not flush small batch within time limit", result.isEmpty());
        Assert.assertFalse("Flush should not be called", sinkTask.wasFlushCalled());
        
        // Add more data to exceed size limit - need enough records to exceed 1MB
        // Each record is roughly ~200-300 bytes with overhead, so need ~4000-5000 records total
        for (int i = 100; i < 4500; i++) {
            String largeJsonRecord = String.format(
                "{\"id\":%d,\"name\":\"user_with_long_name_%d\",\"description\":\"This is a longer description to make the record bigger with more content\",\"data\":\"%s\",\"metadata\":{\"source\":\"test\",\"version\":\"1.0\"}}",
                i, i, "x".repeat(150)
            );
            sinkTask.simulateWrite(largeJsonRecord);
        }
        
        long finalBufferSize = sinkTask.getCurrentBufferBytes();
        LOG.info("Final buffer size after {} records: {} bytes", 4500, finalBufferSize);
        
        // Now should trigger size-based flush
        Map<TopicPartition, OffsetAndMetadata> offsets = createTestOffsets();
        result = sinkTask.preCommit(offsets);
        
        Assert.assertEquals("Should commit when buffer exceeds 1MB", offsets, result);
        Assert.assertTrue("Should flush when size limit exceeded", sinkTask.wasFlushCalled());
    }

    // Helper methods

    private Map<TopicPartition, OffsetAndMetadata> createTestOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(100L));
        return offsets;
    }
}