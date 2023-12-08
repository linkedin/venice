package com.linkedin.venice.serialization;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
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
    assertSame(firstDeserializer, secondDeserializer);
    verify(schemaRepository, times(2)).getValueSchema(anyString(), anyInt());
  }

  @Test
  public void testWithSchemaReader() {
    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema schema = Schema.create(Schema.Type.INT);
    when(schemaReader.getValueSchema(anyInt())).thenReturn(schema);
    AvroStoreDeserializerCache avroStoreDeserializerCache = new AvroStoreDeserializerCache(schemaReader);
    RecordDeserializer firstDeserializer = avroStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(firstDeserializer);
    verify(schemaReader, times(2)).getValueSchema(anyInt());

    // When calling the cache again, we should get the same instance back, and the schema repo should not be re-queried
    RecordDeserializer secondDeserializer = avroStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(secondDeserializer);
    assertSame(firstDeserializer, secondDeserializer);
    verify(schemaReader, times(2)).getValueSchema(anyInt());
  }
}
