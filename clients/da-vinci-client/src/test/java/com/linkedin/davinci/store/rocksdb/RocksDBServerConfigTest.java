package com.linkedin.davinci.store.rocksdb;

import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBServerConfigTest {
  @Test
  public void testDefaultSstFileManagerDeletionRateConfig() {
    VeniceProperties props = new PropertyBuilder().build();
    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerDeleteRateBytesPerSecond(),
        0L,
        "Default deletion rate should be 0 (disabled) for backward compatibility");
  }

  @Test
  public void testDefaultSstFileManagerMaxTrashDBRatio() {
    VeniceProperties props = new PropertyBuilder().build();
    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerMaxTrashDBRatio(),
        0.25,
        "Default max trash DB ratio should be 0.25 (25%)");
  }

  @Test
  public void testCustomSstFileManagerDeletionRate() {
    long expectedRate = 64L * 1024 * 1024; // 64 MB/s
    VeniceProperties props = new PropertyBuilder()
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND, String.valueOf(expectedRate))
        .build();

    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerDeleteRateBytesPerSecond(),
        expectedRate,
        "Custom deletion rate should be respected");
  }

  @Test
  public void testCustomSstFileManagerMaxTrashDBRatio() {
    double expectedRatio = 0.5;
    VeniceProperties props = new PropertyBuilder()
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_MAX_TRASH_DB_RATIO, String.valueOf(expectedRatio))
        .build();

    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerMaxTrashDBRatio(),
        expectedRatio,
        "Custom max trash DB ratio should be respected");
  }

  @Test
  public void testSstFileManagerConfigWithSizeUnits() {
    // Test that size units work correctly (e.g., "32MB", "64MB")
    VeniceProperties props =
        new PropertyBuilder().put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND, "32MB")
            .build();

    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerDeleteRateBytesPerSecond(),
        32L * 1024 * 1024,
        "Size units should be parsed correctly");
  }

  @Test
  public void testZeroDeletionRateDisablesThrottling() {
    VeniceProperties props =
        new PropertyBuilder().put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND, "0")
            .build();

    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerDeleteRateBytesPerSecond(),
        0L,
        "Zero deletion rate should disable throttling");
  }

  @Test
  public void testBothConfigsTogether() {
    long expectedRate = 64L * 1024 * 1024;
    double expectedRatio = 0.3;

    VeniceProperties props = new PropertyBuilder()
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND, String.valueOf(expectedRate))
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_MAX_TRASH_DB_RATIO, String.valueOf(expectedRatio))
        .build();

    RocksDBServerConfig config = new RocksDBServerConfig(props);

    Assert.assertEquals(
        config.getSstFileManagerDeleteRateBytesPerSecond(),
        expectedRate,
        "Deletion rate should be configured correctly when both configs are set");
    Assert.assertEquals(
        config.getSstFileManagerMaxTrashDBRatio(),
        expectedRatio,
        "Max trash DB ratio should be configured correctly when both configs are set");
  }
}
