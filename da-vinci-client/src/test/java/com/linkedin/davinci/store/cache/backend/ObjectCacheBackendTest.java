package com.linkedin.davinci.store.cache.backend;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.cache.VeniceStoreCacheStorageEngine;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import org.testng.Assert;

public class ObjectCacheBackendTest {

    public static final String STORE_NAME = "fooStore";
    public static final String TOPIC_NAME = "fooStore_v1";
    public static final int STORE_VERSION = 1;
    public static final Schema STORE_SCHEMA = Schema.parse("{\"type\":\"record\", \"name\":\"ValueRecord\", \"fields\": [{\"name\":\"number\", "
            + "\"type\":\"int\"}]}");

    @Test
    public void testGetStorageEngine() throws ExecutionException, InterruptedException {
        // Set up Mocks
        ReadOnlySchemaRepository mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
        SchemaEntry entry = new SchemaEntry(0, STORE_SCHEMA);
        Mockito.when(mockSchemaRepo.getKeySchema(STORE_NAME)).thenReturn(entry);
        ObjectCacheConfig cacheConfig = new ObjectCacheConfig();
        ClientConfig mockClientConfig = Mockito.mock(ClientConfig.class);
        ObjectCacheBackend cacheBackend = new ObjectCacheBackend(mockClientConfig, cacheConfig, mockSchemaRepo);

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
        Assert.assertEquals(cacheBackend.get(keyRecord, mockVersion, (k, executor) -> CompletableFuture.completedFuture(cachedValued)).get(), cachedValued);

        // Make sure we get something back
        AbstractStorageEngine cachedStorageEngine = cacheBackend.getStorageEngine(TOPIC_NAME);
        Assert.assertNotNull(cachedStorageEngine);

        //....and make sure it's legit
        Assert.assertEquals(((VeniceStoreCacheStorageEngine) cachedStorageEngine).getCache().getIfPresent(keyRecord), cachedValued);
        Assert.assertEquals(cachedStorageEngine.getStoreName(), TOPIC_NAME);
    }
}