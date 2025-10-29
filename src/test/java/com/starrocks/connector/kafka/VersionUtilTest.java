package com.starrocks.connector.kafka;

import org.junit.Test;
import static org.junit.Assert.*;

public class VersionUtilTest {

    @Test
    public void testConnectorVersionLoading() {
        // Test that connector version is loaded correctly
        assertNotNull("Connector version should not be null", Util.VERSION);
        assertFalse("Connector version should not be empty", Util.VERSION.isEmpty());
        
        // Should be either the Maven version or "unknown" fallback
        assertTrue("Connector version should be valid", 
                  Util.VERSION.equals("2.0.0") || Util.VERSION.equals("unknown"));
    }

    @Test
    public void testStarRocksSDKVersionLoading() {
        // Test that SDK version is loaded correctly
        assertNotNull("SDK version should not be null", Util.STARROCKS_SDK_VERSION);
        assertFalse("SDK version should not be empty", Util.STARROCKS_SDK_VERSION.isEmpty());
        
        // Should be either the Maven property version or "unknown" fallback
        assertTrue("SDK version should be valid", 
                  Util.STARROCKS_SDK_VERSION.equals("1.0") || Util.STARROCKS_SDK_VERSION.equals("unknown"));
    }

    @Test
    public void testVersionIsNotHardcoded() {
        // Test that we're not using the old hardcoded version
        assertNotEquals("Connector version should not be the old hardcoded value", "1.0.3", Util.VERSION);
    }

    @Test
    public void testVersionInfoFormat() {
        // Test the combined version info formatting
        String versionInfo = Util.getVersionInfo();
        assertNotNull("Version info should not be null", versionInfo);
        assertTrue("Version info should contain connector version", versionInfo.contains(Util.VERSION));
        assertTrue("Version info should contain SDK version", versionInfo.contains(Util.STARROCKS_SDK_VERSION));
        assertTrue("Version info should be properly formatted", 
                  versionInfo.startsWith("StarRocks Kafka Connector v"));
    }
}