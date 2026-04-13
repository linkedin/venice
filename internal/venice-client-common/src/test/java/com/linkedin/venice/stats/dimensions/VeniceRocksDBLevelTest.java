package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceRocksDBLevelTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRocksDBLevel, String> expectedValues = CollectionUtils.<VeniceRocksDBLevel, String>mapBuilder()
        .put(VeniceRocksDBLevel.LEVEL_0, "level_0")
        .put(VeniceRocksDBLevel.LEVEL_1, "level_1")
        .put(VeniceRocksDBLevel.LEVEL_2_AND_UP, "level_2_and_up")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceRocksDBLevel.class,
        VeniceMetricsDimensions.VENICE_ROCKSDB_LEVEL,
        expectedValues).assertAll();
  }
}
