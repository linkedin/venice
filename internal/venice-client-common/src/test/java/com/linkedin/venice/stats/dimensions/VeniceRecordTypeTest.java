package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceRecordTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRecordType, String> expectedValues = CollectionUtils.<VeniceRecordType, String>mapBuilder()
        .put(VeniceRecordType.DATA, "data")
        .put(VeniceRecordType.REPLICATION_METADATA, "replication_metadata")
        .build();
    new VeniceDimensionTestFixture<>(VeniceRecordType.class, VeniceMetricsDimensions.VENICE_RECORD_TYPE, expectedValues)
        .assertAll();
  }
}
