package com.linkedin.davinci.repository;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreValueSchema;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DaVinciClientMetaStoreBasedRepositoryTest {
  private DaVinciClientMetaStoreBasedRepository daVinciClientBasedRepository;
  private ClientConfig clientConfig;
  private VeniceProperties backendConfig;
  private CachingDaVinciClientFactory daVinciClientFactory;
  private SchemaReader metaStoreSchemaReader;
  private String clusterName = "test-cluster";

  @BeforeMethod
  public void setupDaVinciBasedRepository() {
    clientConfig = mock(ClientConfig.class);
    backendConfig = mock(VeniceProperties.class);
    doReturn(1L).when(backendConfig).getLong(eq(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS), anyLong());
    daVinciClientFactory = mock(CachingDaVinciClientFactory.class);
    metaStoreSchemaReader = mock(SchemaReader.class);
    daVinciClientBasedRepository = new DaVinciClientMetaStoreBasedRepository(
        clientConfig,
        backendConfig,
        daVinciClientFactory,
        metaStoreSchemaReader);
  }

  private DaVinciClient<StoreMetaKey, StoreMetaValue> setupAndGetBasicMockMetaClient(String storeName)
      throws ExecutionException, InterruptedException {
    DaVinciClient<StoreMetaKey, StoreMetaValue> metaDaVinciClient = mock(DaVinciClient.class);
    doReturn(metaDaVinciClient).when(daVinciClientFactory)
        .getAndStartSpecificAvroClient(
            eq(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName)),
            any(),
            eq(StoreMetaValue.class));
    // Store configs mocks
    CompletableFuture<Void> future = mock(CompletableFuture.class);
    doReturn(future).when(metaDaVinciClient).subscribeAll();
    StoreMetaValue storeConfigValue = new StoreMetaValue();
    storeConfigValue.setStoreClusterConfig(new StoreClusterConfig(clusterName, false, null, null, storeName));
    CompletableFuture<StoreMetaValue> storeConfigFuture = mock(CompletableFuture.class);
    doReturn(storeConfigValue).when(storeConfigFuture).get();
    doReturn(storeConfigFuture).when(metaDaVinciClient)
        .get(
            MetaStoreDataType.STORE_CLUSTER_CONFIG
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    // Store props mocks
    CompletableFuture<StoreMetaValue> storePropFuture = mock(CompletableFuture.class);
    StoreMetaValue storePropValue = new StoreMetaValue();
    StoreProperties storeProperties = new StoreProperties();
    storeProperties.setName(storeName);
    StoreVersion storeVersion = new StoreVersion();
    storeVersion.setStoreName(storeName);
    storeVersion.setNumber(1);
    storeVersion.setPushJobId("test-push");
    storeVersion.setReplicationFactor(1);
    storeVersion.setPartitionCount(1);
    storeProperties.setVersions(Collections.singletonList(storeVersion));
    storeProperties.setCurrentVersion(1);
    storePropValue.setStoreProperties(storeProperties);
    doReturn(storePropValue).when(storePropFuture).get();
    doReturn(storePropFuture).when(metaDaVinciClient)
        .get(MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_CLUSTER_NAME, clusterName);
          }
        }));
    // Store key schema mocks
    CompletableFuture<StoreMetaValue> keySchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue keySchemaValue = new StoreMetaValue();
    Map<CharSequence, CharSequence> keySchemaMap = new HashMap<>();
    keySchemaMap.put("1", "\"string\"");
    keySchemaValue.setStoreKeySchemas(new StoreKeySchemas(keySchemaMap));
    doReturn(keySchemaValue).when(keySchemaFuture).get();
    doReturn(keySchemaFuture).when(metaDaVinciClient)
        .get(
            MetaStoreDataType.STORE_KEY_SCHEMAS
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    // Store value schema mocks
    CompletableFuture<StoreMetaValue> valueSchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue valueSchemaValue = new StoreMetaValue();
    Map<CharSequence, CharSequence> valueSchemaMap = new HashMap<>();
    valueSchemaMap.put("1", "");
    valueSchemaValue.setStoreValueSchemas(new StoreValueSchemas(valueSchemaMap));
    doReturn(valueSchemaValue).when(valueSchemaFuture).get();
    doReturn(valueSchemaFuture).when(metaDaVinciClient)
        .get(
            MetaStoreDataType.STORE_VALUE_SCHEMAS
                .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)));
    CompletableFuture<StoreMetaValue> indiValueSchemaFuture = mock(CompletableFuture.class);
    StoreMetaValue indiSchemaValue = new StoreMetaValue();
    indiSchemaValue.setStoreValueSchema(new StoreValueSchema("\"string\""));
    doReturn(indiSchemaValue).when(indiValueSchemaFuture).get();
    doReturn(indiValueSchemaFuture).when(metaDaVinciClient)
        .get(MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, storeName);
            put(KEY_STRING_SCHEMA_ID, "1");
          }
        }));
    return metaDaVinciClient;
  }

  @Test
  public void testSubscribeAndRefresh() throws InterruptedException, ExecutionException {
    String storeName = "testStore";
    setupAndGetBasicMockMetaClient(storeName);
    daVinciClientBasedRepository.start();
    daVinciClientBasedRepository.subscribe(storeName);
    Assert.assertNotNull(daVinciClientBasedRepository.getStore(storeName));
    Assert.assertEquals(
        daVinciClientBasedRepository.getStore(storeName).getCurrentVersion(),
        1,
        "Unexpected current version");
    Assert.assertEquals(
        daVinciClientBasedRepository.getKeySchema(storeName).getSchemaStr(),
        "\"string\"",
        "Unexpected key schema string");
    Assert.assertEquals(
        daVinciClientBasedRepository.getValueSchema(storeName, 1).getSchemaStr(),
        "\"string\"",
        "Unexpected value schema string");
    daVinciClientBasedRepository.refreshOneStore(storeName);
    Assert.assertNotNull(daVinciClientBasedRepository.getStore(storeName));
    Assert.assertEquals(
        daVinciClientBasedRepository.getStore(storeName).getCurrentVersion(),
        1,
        "Unexpected current version");
    Assert.assertEquals(
        daVinciClientBasedRepository.getKeySchema(storeName).getSchemaStr(),
        "\"string\"",
        "Unexpected key schema string");
    Assert.assertEquals(
        daVinciClientBasedRepository.getValueSchema(storeName, 1).getSchemaStr(),
        "\"string\"",
        "Unexpected value schema string");
  }

  @Test
  public void testSubscribeMetaStoreOnly() throws InterruptedException {
    String storeName = "testStore";
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    DaVinciClientMetaStoreBasedRepository repository = new DaVinciClientMetaStoreBasedRepository(
        clientConfig,
        backendConfig,
        daVinciClientFactory,
        metaStoreSchemaReader);
    DaVinciClientMetaStoreBasedRepository spyRepository = spy(repository);
    SystemStore systemStore = mock(SystemStore.class);
    doReturn(systemStore).when(spyRepository).getMetaStore(metaSystemStoreName);
    spyRepository.subscribe(metaSystemStoreName);
    Assert.assertNotNull(spyRepository.getStore(metaSystemStoreName));
  }
}
