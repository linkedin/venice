package com.linkedin.venice.serialization.avro;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvroSpecificStoreDeserializerCacheTest {
  @Test
  public void testWithSchemaReader() {
    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema schema = Schema.create(Schema.Type.INT);
    when(schemaReader.getValueSchema(anyInt())).thenReturn(schema);

    AvroSpecificStoreDeserializerCache<TestValueRecord> avroSpecificStoreDeserializerCache =
        new AvroSpecificStoreDeserializerCache<>(schemaReader, TestValueRecord.class);
    RecordDeserializer<TestValueRecord> firstDeserializer = avroSpecificStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(firstDeserializer);
    verify(schemaReader, times(1)).getValueSchema(anyInt());

    // When calling the cache again, we should get the same instance back, and the schema repo should not be re-queried
    RecordDeserializer<TestValueRecord> secondDeserializer = avroSpecificStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(secondDeserializer);
    assertSame(firstDeserializer, secondDeserializer);
    verify(schemaReader, times(1)).getValueSchema(anyInt());
  }

  @Test
  public void testWithSchemaRepo() {
    String storeName = "TestStoreName";
    Schema schema = Schema.create(Schema.Type.INT);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    SchemaEntry schemaEntry = mock(SchemaEntry.class);
    when(schemaEntry.getSchema()).thenReturn(schema);
    when(schemaRepository.getValueSchema(eq(storeName), anyInt())).thenReturn(schemaEntry);

    AvroSpecificStoreDeserializerCache<TestValueRecord> avroSpecificStoreDeserializerCache =
        new AvroSpecificStoreDeserializerCache<>(schemaRepository, storeName, TestValueRecord.class);

    RecordDeserializer<TestValueRecord> firstDeserializer = avroSpecificStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(firstDeserializer);
    verify(schemaRepository, times(1)).getValueSchema(eq(storeName), anyInt());

    // When calling the cache again, we should get the same instance back, and the schema repo should not be re-queried
    RecordDeserializer<TestValueRecord> secondDeserializer = avroSpecificStoreDeserializerCache.getDeserializer(1, 1);
    assertNotNull(secondDeserializer);
    assertSame(firstDeserializer, secondDeserializer);
    verify(schemaRepository, times(1)).getValueSchema(eq(storeName), anyInt());
  }
}
