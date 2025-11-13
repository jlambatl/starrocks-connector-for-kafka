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

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.*;

/**
 * Test class for configuration defaults and batching configuration
 */
public class StarRocksSinkConnectorConfigTest {

    @Test
    public void testBatchingConfigurationDefaults() {
        // Create config definition
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        // Get default values
        Map<String, Object> defaults = configDef.defaultValues();
        
        // Verify new improved default for flush interval (30 seconds instead of 1 second)
        Object defaultFlushInterval = defaults.get(BUFFERFLUSH_INTERVALMS);
        Assert.assertNotNull("Flush interval default should be defined", defaultFlushInterval);
        Assert.assertEquals("Default flush interval should be 30000ms for better batching", 
                          30000L, defaultFlushInterval);
        
        // Verify max bytes default remains unchanged (64MB)
        Object defaultMaxBytes = defaults.get(BUFFERFLUSH_MAXBYTES);
        Assert.assertNotNull("Max bytes default should be defined", defaultMaxBytes);
        Assert.assertEquals("Default max bytes should remain 64MB", 
                          67108864L, defaultMaxBytes);
    }

    @Test
    public void testBatchingConfigurationValidation() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        // Test flush interval validation
        ConfigDef.ConfigKey flushIntervalKey = configDef.configKeys().get(BUFFERFLUSH_INTERVALMS);
        Assert.assertNotNull("Flush interval config should exist", flushIntervalKey);
        Assert.assertEquals("Flush interval should be LONG type", ConfigDef.Type.LONG, flushIntervalKey.type);
        
        // Test max bytes validation  
        ConfigDef.ConfigKey maxBytesKey = configDef.configKeys().get(BUFFERFLUSH_MAXBYTES);
        Assert.assertNotNull("Max bytes config should exist", maxBytesKey);
        Assert.assertEquals("Max bytes should be LONG type", ConfigDef.Type.LONG, maxBytesKey.type);
    }

    @Test
    public void testConnectorValidationSetsDefaults() {
        StarRocksSinkConnector connector = new StarRocksSinkConnector();
        
        // Test configuration without explicit batching settings
        Map<String, String> config = new java.util.HashMap<>();
        config.put(STARROCKS_LOAD_URL, "http://localhost:8030");
        config.put(STARROCKS_DATABASE_NAME, "test_db");
        config.put(STARROCKS_USERNAME, "root");
        config.put(STARROCKS_PASSWORD, "");
        
        // Note: Not setting BUFFERFLUSH_INTERVALMS or BUFFERFLUSH_MAXBYTES
        
        // Validate should set defaults
        connector.validate(config);
        
        // Check that defaults were applied
        Assert.assertTrue("Should contain flush interval after validation", 
                        config.containsKey(BUFFERFLUSH_INTERVALMS));
        Assert.assertEquals("Should set new default flush interval", 
                          "30000", config.get(BUFFERFLUSH_INTERVALMS));
        
        Assert.assertTrue("Should contain max bytes after validation", 
                        config.containsKey(BUFFERFLUSH_MAXBYTES));
        Assert.assertEquals("Should set default max bytes", 
                          "67108864", config.get(BUFFERFLUSH_MAXBYTES));
    }

    @Test 
    public void testBatchingConfigurationRanges() {
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        // Test flush interval range
        ConfigDef.ConfigKey flushIntervalKey = configDef.configKeys().get(BUFFERFLUSH_INTERVALMS);
        Assert.assertTrue("Flush interval should have range validator", 
                        flushIntervalKey.validator instanceof ConfigDef.Range);
        
        // Test max bytes range
        ConfigDef.ConfigKey maxBytesKey = configDef.configKeys().get(BUFFERFLUSH_MAXBYTES);
        Assert.assertTrue("Max bytes should have range validator", 
                        maxBytesKey.validator instanceof ConfigDef.Range);
    }
}