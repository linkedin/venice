package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryTest {
  private ClientConfig clientConfigThinClient;
  private ClientConfig clientConfigRequestBased;
  private VeniceProperties backendConfig;
  private MetricsRepository metricsRepository;
  private Clock clock;
  private static final String STORE_NAME = "hardware_store";

  @BeforeMethod
  public void setUpMocks() {
    clientConfigThinClient = mock(ClientConfig.class);
    clientConfigRequestBased = mock(ClientConfig.class);
    backendConfig = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfig).getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    metricsRepository = new MetricsRepository();
    doReturn(metricsRepository).when(clientConfigThinClient).getMetricsRepository();
    doReturn(metricsRepository).when(clientConfigRequestBased).getMetricsRepository();
    doReturn(true).when(clientConfigRequestBased).isUseRequestBasedMetaRepository();
    clock = mock(Clock.class);
    doReturn(0L).when(clock).millis();
  }

  @Test
  public void testGetThinClientInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfigThinClient, backendConfig);
    Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(() -> nativeMetadataRepository.start());
  }

  @Test
  public void testGetRequestBasedInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfigRequestBased, backendConfig);
    Assert.assertTrue(nativeMetadataRepository instanceof RequestBasedMetaRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(() -> nativeMetadataRepository.start());
  }

  @Test
  public void testGetSchemaDataFromReadThroughCache() throws InterruptedException {
    TestNMR nmr = new TestNMR(clientConfigThinClient, backendConfig, clock);
    nmr.start();
    Assert.assertThrows(VeniceNoStoreException.class, () -> nmr.getKeySchema(STORE_NAME));
    nmr.subscribe(STORE_NAME);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
  }

  @Test
  public void testGetSchemaDataEfficiently() throws InterruptedException {
    doReturn(Long.MAX_VALUE).when(backendConfig)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    TestNMR nmr = new TestNMR(clientConfigThinClient, backendConfig, clock);
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
    TestNMR nmr = new TestNMR(clientConfigThinClient, backendConfig, clock);
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
    protected StoreConfig fetchStoreConfigFromRemote(String storeName) {
      StoreConfig storeConfig = mock(StoreConfig.class);
      when(storeConfig.isDeleting()).thenReturn(false);
      return storeConfig;
    }

    @Override
    protected Store fetchStoreFromRemote(String storeName, String clusterName) {
      Store store = mock(Store.class);
      when(store.getName()).thenReturn(storeName);
      when(store.getReadQuotaInCU()).thenReturn(1L);
      return store;
    }

    @Override
    protected SchemaData getSchemaData(String storeName) {
      SchemaData schemaData = schemaMap.get(storeName);
      SchemaEntry keySchema;
      if (schemaData == null) {
        // Retrieve the key schema and initialize SchemaData only if it's not cached yet.
        StoreMetaKey keySchemaKey = MetaStoreDataType.STORE_KEY_SCHEMAS
            .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
        Map<CharSequence, CharSequence> keySchemaMap =
            getStoreMetaValue(storeName, keySchemaKey).storeKeySchemas.keySchemaMap;
        if (keySchemaMap.isEmpty()) {
          throw new VeniceException("No key schema found for store: " + storeName);
        }
        Map.Entry<CharSequence, CharSequence> keySchemaEntry = keySchemaMap.entrySet().iterator().next();
        keySchema =
            new SchemaEntry(Integer.parseInt(keySchemaEntry.getKey().toString()), keySchemaEntry.getValue().toString());
        schemaData = new SchemaData(storeName, keySchema);
      }
      StoreMetaKey valueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMAS
          .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
      Map<CharSequence, CharSequence> valueSchemaMap =
          getStoreMetaValue(storeName, valueSchemaKey).storeValueSchemas.valueSchemaMap;
      // Check the value schema string, if it's empty then try to query the other key space for individual value schema.
      for (Map.Entry<CharSequence, CharSequence> entry: valueSchemaMap.entrySet()) {
        // Check if we already have the corresponding value schema
        int valueSchemaId = Integer.parseInt(entry.getKey().toString());
        if (schemaData.getValueSchema(valueSchemaId) != null) {
          continue;
        }
        if (entry.getValue().toString().isEmpty()) {
          // The value schemas might be too large to be stored in a single K/V.
          StoreMetaKey individualValueSchemaKey =
              MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
                {
                  put(KEY_STRING_STORE_NAME, storeName);
                  put(KEY_STRING_SCHEMA_ID, entry.getKey().toString());
                }
              });
          // Empty string is not a valid value schema therefore it's safe to throw exceptions if we also cannot find it
          // in
          // the individual value schema key space.
          String valueSchema =
              getStoreMetaValue(storeName, individualValueSchemaKey).storeValueSchema.valueSchema.toString();
          schemaData.addValueSchema(new SchemaEntry(valueSchemaId, valueSchema));
        } else {
          schemaData.addValueSchema(new SchemaEntry(valueSchemaId, entry.getValue().toString()));
        }
      }
      return schemaData;
    }

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
  }
}
