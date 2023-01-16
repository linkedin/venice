package com.linkedin.davinci.store.cache.backend;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.VeniceStoreCacheStorageEngine;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ObjectCacheBackendTest {
  public static final String STORE_NAME = "fooStore";
  public static final String TOPIC_NAME = "fooStore_v1";
  public static final int STORE_VERSION = 1;
  static final List<Integer> NEW_STORE_VERSIONS = Arrays.asList(2, 3);
  public static final Schema STORE_SCHEMA = Schema.parse(
      "{\"type\":\"record\", \"name\":\"ValueRecord\", \"fields\": [{\"name\":\"number\", " + "\"type\":\"int\"}]}");

  @Test
  public void testGetStorageEngine() throws ExecutionException, InterruptedException {
    // Set up Mocks
    ReadOnlySchemaRepository mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    SchemaEntry entry = new SchemaEntry(0, STORE_SCHEMA);
    Mockito.when(mockSchemaRepo.getKeySchema(STORE_NAME)).thenReturn(entry);
    ObjectCacheConfig cacheConfig = new ObjectCacheConfig();
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(Utils.getUniqueString());
    ObjectCacheBackend cacheBackend = new ObjectCacheBackend(clientConfig, cacheConfig, mockSchemaRepo);

    // Set up a mock version
    Version mockVersion = Mockito.mock(Version.class);
    Mockito.when(mockVersion.getStoreName()).thenReturn(STORE_NAME);
    Mockito.when(mockVersion.getNumber()).thenReturn(STORE_VERSION);
    Mockito.when(mockVersion.kafkaTopicName()).thenReturn(TOPIC_NAME);

    // Test getting a storage engine which shouldn't exist yet (nothings been written yet)
    Assert.assertNull(cacheBackend.getStorageEngine(TOPIC_NAME));

    // Populate a storage engine now
    GenericRecord keyRecord = new GenericData.Record(STORE_SCHEMA);
    keyRecord.put("number", 1);
    Integer cachedValued = 0;
    Assert.assertEquals(
        cacheBackend.get(keyRecord, mockVersion, (k, executor) -> CompletableFuture.completedFuture(cachedValued))
            .get(),
        cachedValued);

    // Make sure we get something back
    AbstractStorageEngine cachedStorageEngine = cacheBackend.getStorageEngine(TOPIC_NAME);
    Assert.assertNotNull(cachedStorageEngine);

    // ....and make sure it's legit
    Assert.assertEquals(
        ((VeniceStoreCacheStorageEngine) cachedStorageEngine).getCache().getIfPresent(keyRecord),
        cachedValued);
    Assert.assertEquals(cachedStorageEngine.getStoreName(), TOPIC_NAME);

    // Make a version push happen that invalidates entries (by triggering the store change event)
    Store mockStore = Mockito.mock(Store.class);
    Mockito.when(mockStore.getName()).thenReturn(STORE_NAME);
    List<Version> newStoreVersions = new ArrayList<>();
    for (Integer version: NEW_STORE_VERSIONS) {
      Version newMockVersion = Mockito.mock(Version.class);
      Mockito.when(newMockVersion.getStoreName()).thenReturn(STORE_NAME);
      Mockito.when(newMockVersion.getNumber()).thenReturn(version);
      Mockito.when(newMockVersion.kafkaTopicName()).thenReturn(TOPIC_NAME.replace("1", String.valueOf(version)));
      newStoreVersions.add(newMockVersion);
    }
    Mockito.when(mockStore.getVersions()).thenReturn(newStoreVersions);

    // Trigger the store change
    cacheBackend.getCacheInvalidatingStoreChangeListener().handleStoreChanged(mockStore);

    // Make sure the previous version was cleared out
    cachedStorageEngine = cacheBackend.getStorageEngine(TOPIC_NAME);
    Assert.assertNull(cachedStorageEngine);

    // Drop it (and don't throw an exception)
    cacheBackend.getCacheInvalidatingStoreChangeListener().handleStoreDeleted(mockStore);
  }
}
