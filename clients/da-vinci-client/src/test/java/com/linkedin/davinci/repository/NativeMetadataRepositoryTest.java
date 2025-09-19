package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.stats.NativeMetadataRepositoryStats;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryTest {
  private ClientConfig clientConfig;

  private VeniceProperties backendConfigThinClient;
  private VeniceProperties backendConfigRequestBased;

  private MetricsRepository metricsRepository;
  private Clock clock;
  private static final String STORE_NAME = "hardware_store";

  @BeforeMethod
  public void setUpMocks() {
    clientConfig = mock(ClientConfig.class);

    backendConfigThinClient = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfigThinClient)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());

    backendConfigRequestBased = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfigRequestBased)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    doReturn(true).when(backendConfigRequestBased)
        .getBoolean(eq(CLIENT_USE_REQUEST_BASED_METADATA_REPOSITORY), anyBoolean());

    metricsRepository = new MetricsRepository();
    doReturn(metricsRepository).when(clientConfig).getMetricsRepository();
    clock = mock(Clock.class);
    doReturn(0L).when(clock).millis();
  }

  @Test
  public void testGetThinClientInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfig, backendConfigThinClient);
    Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(nativeMetadataRepository::start);
  }

  @Test
  public void testGetRequestBasedInstance() {
    NativeMetadataRepository nativeMetadataRepository =
        NativeMetadataRepository.getInstance(clientConfig, backendConfigRequestBased);
    Assert.assertTrue(nativeMetadataRepository instanceof RequestBasedMetaRepository);

    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    nativeMetadataRepository.start();
    nativeMetadataRepository.clear();
    Assert.assertThrows(() -> nativeMetadataRepository.subscribe(STORE_NAME));
    Assert.assertThrows(nativeMetadataRepository::start);
  }

  @Test
  public void testGetSchemaDataFromReadThroughCache() throws InterruptedException {
    TestNMR nmr = new TestNMR(clientConfig, backendConfigThinClient, clock);
    nmr.start();
    Assert.assertThrows(VeniceNoStoreException.class, () -> nmr.getKeySchema(STORE_NAME));
    nmr.subscribe(STORE_NAME);
    Assert.assertNotNull(nmr.getKeySchema(STORE_NAME));
  }

  @Test
  public void testGetSchemaDataEfficiently() throws InterruptedException {
    doReturn(Long.MAX_VALUE).when(backendConfigThinClient)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    TestNMR nmr = new TestNMR(clientConfig, backendConfigThinClient, clock);
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
    for (int i = 0; i < 10; i++) {
      nmr.refreshOneStore(STORE_NAME);
    }
    Assert.assertEquals(nmr.keySchemaRequestCount, 1);
    Assert.assertEquals(nmr.valueSchemasRequestCount, 12);
    Assert.assertEquals(nmr.specificValueSchemaRequestCount, 1);
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
    doReturn(Long.MAX_VALUE).when(backendConfigThinClient)
        .getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    TestNMR nmr = new TestNMR(clientConfig, backendConfigThinClient, clock);
    nmr.start();
    nmr.subscribe(STORE_NAME);
    doReturn(1000L).when(clock).millis();

    NativeMetadataRepositoryStats nativeMetadataRepository = nmr.getNativeMetadataRepositoryStats();
    Assert.assertNotNull(nativeMetadataRepository);
    Assert.assertEquals(nativeMetadataRepository.getMetadataStalenessHighWatermarkMs(), 1000d);

    String anotherStoreName = STORE_NAME + "V2";
    nmr.subscribe(anotherStoreName);
    nmr.refreshOneStore(anotherStoreName);
    // After one store refresh we should still see staleness increase because it reports the max amongst all stores
    doReturn(2000L).when(clock).millis();
    Assert.assertEquals(nmr.getNativeMetadataRepositoryStats().getMetadataStalenessHighWatermarkMs(), 2000d);

    // Refresh both stores and staleness should decrease
    nmr.refresh();
    Assert.assertEquals(nmr.getNativeMetadataRepositoryStats().getMetadataStalenessHighWatermarkMs(), 0d);

    // Unsubscribing stores should remove their corresponding staleness metric
    nmr.unsubscribe(STORE_NAME);
    nmr.unsubscribe(anotherStoreName);
    Assert.assertEquals(nmr.getNativeMetadataRepositoryStats().getMetadataStalenessHighWatermarkMs(), Double.NaN);
  }

  static class TestNMR extends NativeMetadataRepository {
    int keySchemaRequestCount = 0;
    int valueSchemasRequestCount = 0;
    int specificValueSchemaRequestCount = 0;

    private static final String INT_KEY_SCHEMA = "\"int\"";

    private static final String VALUE_SCHEMA_1 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
        + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"}\n" + "  ]\n" + "}";
    private static final String VALUE_SCHEMA_2 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
        + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"},\n"
        + "   {\"name\": \"test_field2\", \"type\": \"int\", \"default\": 0}\n" + "  ]\n" + "}";

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
      if (schemaMap.containsKey(storeName)) {
        valueSchemasRequestCount++;
        return schemaMap.get(storeName);
      }

      // Mock schemas for testing
      SchemaEntry schemaEntry = new SchemaEntry(0, INT_KEY_SCHEMA);
      SchemaData schemaData = new SchemaData(storeName, schemaEntry);
      schemaData.addValueSchema(new SchemaEntry(1, VALUE_SCHEMA_1));
      schemaData.addValueSchema(new SchemaEntry(2, VALUE_SCHEMA_2));

      // Mock metrics
      keySchemaRequestCount++;
      valueSchemasRequestCount++;
      specificValueSchemaRequestCount++;

      schemaMap.put(storeName, schemaData);
      return schemaData;
    }
  }
}
