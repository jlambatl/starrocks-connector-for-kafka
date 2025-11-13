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
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for StarRocks connector configuration, specifically batching-related defaults
 */
public class StarRocksConnectorConfigTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksConnectorConfigTest.class);

    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    @Test
    public void testDefaultBufferFlushIntervalConfiguration() {
        // Test the new default flush interval in the ConfigDef
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        // Get the default value for buffer flush interval
        Object defaultValue = configDef.defaultValues().get(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS);
        
        Assert.assertNotNull("Buffer flush interval should have a default value", defaultValue);
        Assert.assertEquals("Default buffer flush interval should be 30000ms", 30000L, defaultValue);
        
        LOG.info("Verified default buffer flush interval: {} ms", defaultValue);
    }

    @Test
    public void testDefaultBufferFlushMaxBytesConfiguration() {
        // Test the default max bytes configuration
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        Object defaultValue = configDef.defaultValues().get(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES);
        
        Assert.assertNotNull("Buffer flush max bytes should have a default value", defaultValue);
        Assert.assertEquals("Default buffer flush max bytes should be 67108864", 67108864L, defaultValue);
        
        LOG.info("Verified default buffer flush max bytes: {} bytes", defaultValue);
    }

    @Test
    public void testConnectorValidationSetsDefaults() {
        // Test that the connector validation properly sets default values
        StarRocksSinkConnector connector = new StarRocksSinkConnector();
        Map<String, String> config = new HashMap<>();
        
        // Set only required configs
        config.put(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL, "localhost:8030");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME, "test_db");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_USERNAME, "test_user");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD, "test_password");
        
        // Validate - this should add default values
        connector.validate(config);
        
        // Verify all expected defaults are set
        Assert.assertEquals("Should set default buffer flush interval", 
                           "30000", config.get(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS));
        Assert.assertEquals("Should set default buffer flush max bytes", 
                           "67108864", config.get(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES));
        Assert.assertEquals("Should set default connect timeout", 
                           "100", config.get(StarRocksSinkConnectorConfig.CONNECT_TIMEOUTMS));
        Assert.assertEquals("Should set default max retries", 
                           "3", config.get(StarRocksSinkConnectorConfig.SINK_MAXRETRIES));
        
        LOG.info("Verified all default values are set correctly through validation");
    }

    @Test
    public void testConnectorValidationRespectsProvidedValues() {
        // Test that validation doesn't override user-provided values
        StarRocksSinkConnector connector = new StarRocksSinkConnector();
        Map<String, String> config = new HashMap<>();
        
        // Set required configs
        config.put(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL, "localhost:8030");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME, "test_db");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_USERNAME, "test_user");
        config.put(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD, "test_password");
        
        // Set custom values for batching
        config.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "60000"); // 1 minute
        config.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES, "134217728"); // 128MB
        
        // Validate
        connector.validate(config);
        
        // Verify custom values are preserved
        Assert.assertEquals("Should preserve custom buffer flush interval", 
                           "60000", config.get(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS));
        Assert.assertEquals("Should preserve custom buffer flush max bytes", 
                           "134217728", config.get(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES));
        
        LOG.info("Verified that custom configuration values are preserved");
    }

    @Test
    public void testConfigurationRanges() {
        // Test that configuration validation respects valid ranges
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        // Test buffer flush interval range
        Map<String, Object> testConfig = new HashMap<>();
        testConfig.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, 500L); // Below minimum of 1000
        
        try {
            configDef.parse(testConfig);
            Assert.fail("Should reject values below minimum range");
        } catch (Exception e) {
            LOG.info("Correctly rejected invalid buffer flush interval: {}", e.getMessage());
        }
        
        // Test valid value
        testConfig.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, 30000L);
        Map<String, Object> parsed = configDef.parse(testConfig);
        Assert.assertEquals("Should accept valid buffer flush interval", 
                           30000L, parsed.get(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS));
    }

    @Test
    public void testBatchingConfigurationDocumentation() {
        // Test that configuration has proper documentation
        ConfigDef configDef = StarRocksSinkConnectorConfig.newConfigDef();
        
        String intervalDoc = configDef.configKeys().get(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS).documentation;
        String maxBytesDoc = configDef.configKeys().get(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES).documentation;
        
        Assert.assertNotNull("Buffer flush interval should have documentation", intervalDoc);
        Assert.assertNotNull("Buffer flush max bytes should have documentation", maxBytesDoc);
        
        Assert.assertTrue("Interval documentation should mention bulk/batch", 
                         intervalDoc.toLowerCase().contains("bulk") || intervalDoc.toLowerCase().contains("batch"));
        Assert.assertTrue("Max bytes documentation should mention batch", 
                         maxBytesDoc.toLowerCase().contains("batch"));
        
        LOG.info("Verified configuration documentation is present and relevant");
    }
}