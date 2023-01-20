package com.linkedin.venice.store.cache;

import com.linkedin.davinci.store.cache.VeniceStoreCacheStoragePartition;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceStoreCacheStoragePartitionTest {
  Schema keySchema = Schema.create(Schema.Type.INT);
  Schema valueSchema = Schema.create(Schema.Type.INT);
  RecordSerializer<Integer> keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
  RecordSerializer<Integer> valueSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
  VeniceStoreCacheStoragePartition cacheStoragePartition;
  private static Integer PRESENT_KEY = 20;
  private static Integer VALUE = 10;
  private static Integer NEW_VALUE = 30;

  @BeforeMethod
  public void makeCache() {
    // Make Some Cache!
    ObjectCacheConfig cacheConfig = new ObjectCacheConfig();
    cacheStoragePartition = new VeniceStoreCacheStoragePartition(0, cacheConfig, keySchema, (k, executor) -> {
      return null;
    });
  }

  @AfterMethod
  public void cleanUp() {
    cacheStoragePartition.drop();
    cacheStoragePartition.close();
  }

  @Test
  public void testCacheStoragePartitionCrud() throws ExecutionException, InterruptedException {
    // Put the deserialized value
    cacheStoragePartition.put(PRESENT_KEY, VALUE);

    // Get the deserialized value
    Assert.assertEquals(cacheStoragePartition.getVeniceCache().get(PRESENT_KEY).get(), VALUE);

    // Put a serialized value like the ingestion service would use (this should invalidate the entry, not make a new
    // one)
    cacheStoragePartition.put(keySerializer.serialize(PRESENT_KEY), valueSerializer.serialize(NEW_VALUE));

    // Check to make sure the value is gone.
    Assert.assertNull(cacheStoragePartition.getVeniceCache().getIfPresent(PRESENT_KEY));

    // Update an existing value
    cacheStoragePartition.put(PRESENT_KEY, VALUE);
    Assert.assertEquals(cacheStoragePartition.getVeniceCache().get(PRESENT_KEY).get(), VALUE);
    cacheStoragePartition.put(PRESENT_KEY, NEW_VALUE);
    Assert.assertEquals(cacheStoragePartition.getVeniceCache().get(PRESENT_KEY).get(), NEW_VALUE);
  }
}
