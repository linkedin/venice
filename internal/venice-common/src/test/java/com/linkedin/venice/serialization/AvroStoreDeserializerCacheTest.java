package com.linkedin.venice.serialization;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.DataProviderUtils;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvroStoreDeserializerCacheTest {
  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void test(boolean fastAvroEnabled) {
    // Setup
    Schema schema = Schema.create(Schema.Type.INT);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    SchemaEntry schemaEntry = mock(SchemaEntry.class);
    when(schemaEntry.getSchema()).thenReturn(schema);
    when(schemaRepository.getValueSchema(anyString(), anyInt())).thenReturn(schemaEntry);

    // Call the cache a first time
    AvroStoreDeserializerCache avroStoreDeserializerCache =
        new AvroStoreDeserializerCache(schemaRepository, "storeName", fastAvroEnabled);
    RecordDeserializer firstDeserializer = avroStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(firstDeserializer);
    verify(schemaRepository, times(2)).getValueSchema(anyString(), anyInt());

    // When calling the cache again, we should get the same instance back, and the schema repo should not be re-queried
    RecordDeserializer secondDeserializer = avroStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(secondDeserializer);
    assertTrue(firstDeserializer == secondDeserializer);
    verify(schemaRepository, times(2)).getValueSchema(anyString(), anyInt());
  }
}
