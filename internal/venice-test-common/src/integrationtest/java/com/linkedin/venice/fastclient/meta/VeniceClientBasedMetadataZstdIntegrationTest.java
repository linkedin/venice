package com.linkedin.venice.fastclient.meta;

import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class VeniceClientBasedMetadataZstdIntegrationTest extends VeniceClientBasedMetadataIntegrationTest {
  protected void createStore() throws Exception {
    storeName = veniceCluster.createStoreWithZstdDictionary(KEY_COUNT);
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadataZstdDictionaryFetch() {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    ReadOnlyStoreRepository storeRepository = routerWrapper.getMetaDataRepository();
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertEquals(
            veniceClientBasedMetadata.getCurrentStoreVersion(),
            storeRepository.getStore(storeName).getCurrentVersion()));
    VeniceCompressor compressor = veniceClientBasedMetadata
        .getCompressor(CompressionStrategy.ZSTD_WITH_DICT, storeRepository.getStore(storeName).getCurrentVersion());
    assertNotNull(compressor);
    ClusterStats clusterStats = clientConfig.getClusterStats();
    List<Double> version_update_failure = clusterStats.getMetricValues("version_update_failure", "OccurrenceRate");
    List<Double> current_version = clusterStats.getMetricValues("current_version", "Gauge");
    assertEquals(version_update_failure.size(), 1, "Unexpected statistic size");
    assertEquals(version_update_failure.get(0), 0.0, "Unexpected version update failure");
    assertEquals(current_version.size(), 1, "Unexpected statistic size");
    assertEquals(current_version.get(0), 1.0, "Unexpected version number");
  }

  public void testMetadata() {
    // Empty implementation to force superclass test not to run. Superclass test versions don't use dictionary.
  }

}
