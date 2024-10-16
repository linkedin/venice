package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreValueSchema;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryTest {
  private ClientConfig clientConfig;
  private VeniceProperties backendConfig;
  private MetricsRepository metricsRepository;
  private Clock clock;
  private static final String STORE_NAME = "hardware_store";

  @BeforeMethod
  public void setUpMocks() {
    clientConfig = mock(ClientConfig.class);
    backendConfig = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfig).getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    metricsRepository = new MetricsRepository();
    doReturn(metricsRepository).when(clientConfig).getMetricsRepository();
    clock = mock(Clock.class);
    doReturn(0L).when(clock).millis();
  }

  @Test
  public void testGetInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfig, backendConfig);
    Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(() -> nativeMetadataRepository.start());
  }

  @Test
  public void testGetSchemaDataFromReadThroughCache() throws InterruptedException {
    TestNMR nmr = new TestNMR(clientConfig, backendConfig, clock);
    nmr.start();
    Assert.assertThrows(VeniceNoStoreException.class, () -> nmr.getKeySchema(STORE_NAME));
    nmr.subscribe(STORE_NAME);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
  }

  @Test
  public void testGetSchemaDataEfficiently() throws InterruptedException {
    doReturn(Long.MAX_VALUE).when(backendConfig)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    TestNMR nmr = new TestNMR(clientConfig, backendConfig, clock);
    nmr.start();
    Assert.assertEquals(nmr.keySchemaRequestCount, 0);
    Assert.assertEquals(nmr.valueSchemasRequestCount, 0);
    Assert.assertEquals(nmr.specificValueSchemaRequestCount, 0);
    nmr.subscribe(STORE_NAME);
    Assert.assertEquals(nmr.keySchemaRequestCount, 1);
    Assert.assertEquals(nmr.valueSchemasRequestCount, 1);
    Assert.assertEquals(nmr.specificValueSchemaRequestCount, 1);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
    Assert.assertNotNull(nmr.getValueSchema(STORE_NAME, 1));
    // Refresh the store, we are not expecting to retrieve key schema or any specific value schema again.
    nmr.refreshOneStore(STORE_NAME);
    Assert.assertEquals(nmr.keySchemaRequestCount, 1);
    Assert.assertEquals(nmr.valueSchemasRequestCount, 2);
    Assert.assertEquals(nmr.specificValueSchemaRequestCount, 1);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
    Assert.assertNotNull(nmr.getValueSchema(STORE_NAME, 1));
    // Refresh the store a few more times to retrieve value schema v2
    for (int i = 0; i < 10; i++) {
      nmr.refreshOneStore(STORE_NAME);
    }
    Assert.assertEquals(nmr.keySchemaRequestCount, 1);
    Assert.assertEquals(nmr.valueSchemasRequestCount, 12);
    Assert.assertEquals(nmr.specificValueSchemaRequestCount, 2);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
    Assert.assertNotNull(nmr.getValueSchema(STORE_NAME, 1));
    Assert.assertNotNull(nmr.getValueSchema(STORE_NAME, 2));
  }

  /**
   * We are using {@link TestUtils#waitForNonDeterministicAssertion(long, TimeUnit, TestUtils.NonDeterministicAssertion)}
   * because in some rare cases the underlying async gauge will return stale value
   */
  @Test
  public void testNativeMetadataRepositoryStats() throws InterruptedException {
    doReturn(Long.MAX_VALUE).when(backendConfig)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    TestNMR nmr = new TestNMR(clientConfig, backendConfig, clock);
    nmr.start();
    nmr.subscribe(STORE_NAME);
    doReturn(1000L).when(clock).millis();
    String stalenessMetricName = ".native_metadata_repository--store_metadata_staleness_high_watermark_ms.Gauge";
    TestUtils.waitForNonDeterministicAssertion(500, TimeUnit.MILLISECONDS, () -> {
      Metric staleness = metricsRepository.getMetric(stalenessMetricName);
      Assert.assertNotNull(staleness);
      Assert.assertEquals(staleness.value(), 1000d);
    });
    String anotherStoreName = STORE_NAME + "V2";
    nmr.subscribe(anotherStoreName);
    nmr.refreshOneStore(anotherStoreName);
    // After one store refresh we should still see staleness increase because it reports the max amongst all stores
    doReturn(2000L).when(clock).millis();
    TestUtils.waitForNonDeterministicAssertion(
        500,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(metricsRepository.getMetric(stalenessMetricName).value(), 2000d));
    // Refresh both stores and staleness should decrease
    nmr.refresh();
    TestUtils.waitForNonDeterministicAssertion(
        500,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(metricsRepository.getMetric(stalenessMetricName).value(), 0d));
    nmr.unsubscribe(STORE_NAME);
    nmr.unsubscribe(anotherStoreName);
    // Unsubscribing stores should remove their corresponding staleness metric
    TestUtils.waitForNonDeterministicAssertion(
        500,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(metricsRepository.getMetric(stalenessMetricName).value(), Double.NaN));
  }

  static class TestNMR extends NativeMetadataRepository {
    int keySchemaRequestCount = 0;
    int valueSchemasRequestCount = 0;
    int specificValueSchemaRequestCount = 0;

    protected TestNMR(ClientConfig clientConfig, VeniceProperties backendConfig, Clock clock) {
      super(clientConfig, backendConfig, clock);
    }

    @Override
    protected StoreConfig getStoreConfigFromSystemStore(String storeName) {
      StoreConfig storeConfig = mock(StoreConfig.class);
      when(storeConfig.isDeleting()).thenReturn(false);
      return storeConfig;
    }

    @Override
    protected Store getStoreFromSystemStore(String storeName, String clusterName) {
      Store store = mock(Store.class);
      when(store.getName()).thenReturn(storeName);
      when(store.getReadQuotaInCU()).thenReturn(1L);
      return store;
    }

    @Override
    protected StoreMetaValue getStoreMetaValue(String storeName, StoreMetaKey key) {
      StoreMetaValue storeMetaValue = new StoreMetaValue();
      MetaStoreDataType metaStoreDataType = MetaStoreDataType.valueOf(key.metadataType);
      switch (metaStoreDataType) {
        case STORE_KEY_SCHEMAS:
          Map<CharSequence, CharSequence> keySchemaMap = new HashMap<>();
          keySchemaMap.put(String.valueOf(1), TestKeyRecord.SCHEMA$.toString());
          storeMetaValue.storeKeySchemas = new StoreKeySchemas(keySchemaMap);
          keySchemaRequestCount++;
          break;
        case STORE_VALUE_SCHEMAS:
          Map<CharSequence, CharSequence> valueSchemaMap = new HashMap<>();
          valueSchemaMap.put(String.valueOf(1), "");
          if (valueSchemasRequestCount > 1) {
            valueSchemaMap.put(String.valueOf(2), "");
          }
          storeMetaValue.storeValueSchemas = new StoreValueSchemas(valueSchemaMap);
          valueSchemasRequestCount++;
          break;
        case STORE_VALUE_SCHEMA:
          storeMetaValue.storeValueSchema = new StoreValueSchema(TestValueRecord.SCHEMA$.toString());
          specificValueSchemaRequestCount++;
          break;
        default:
          // do nothing
      }
      return storeMetaValue;
    }

    @Override
    protected SchemaData getSchemaDataFromSystemStore(String storeName) {
      return getSchemaDataFromMetaSystemStore(storeName);
    }
  }
}
