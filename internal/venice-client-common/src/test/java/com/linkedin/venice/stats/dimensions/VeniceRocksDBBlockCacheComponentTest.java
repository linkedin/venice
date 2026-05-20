package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceRocksDBBlockCacheComponentTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRocksDBBlockCacheComponent, String> expectedValues =
        CollectionUtils.<VeniceRocksDBBlockCacheComponent, String>mapBuilder()
            .put(VeniceRocksDBBlockCacheComponent.INDEX, "index")
            .put(VeniceRocksDBBlockCacheComponent.FILTER, "filter")
            .put(VeniceRocksDBBlockCacheComponent.DATA, "data")
            .put(VeniceRocksDBBlockCacheComponent.COMPRESSION_DICT, "compression_dict")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceRocksDBBlockCacheComponent.class,
        VeniceMetricsDimensions.VENICE_ROCKSDB_BLOCK_CACHE_COMPONENT,
        expectedValues).assertAll();
  }
}
