package com.linkedin.davinci.replication.merge;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCreateOldValueAndRmdWithCache extends TestMergeConflictResolver {
  /**
   * Verifies that when a cached GenericRecord is provided and its schema ID matches the reader schema ID,
   * the cached record is reused directly (same object identity), skipping deserialization.
   */
  @Test
  public void testCacheHitWithMatchingSchemaSkipsDeserialization() {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, userSchemaV1)).when(schemaRepository).getValueSchema(storeName, 1);
    StringAnnotatedStoreSchemaCache schemaCache = new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);

    MergeConflictResolver resolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(schemaCache, new RmdSerDe(schemaCache, RMD_VERSION_ID), storeName, true, false);

    // Create a cached GenericRecord in schema V1
    GenericRecord cachedRecord = new GenericData.Record(userSchemaV1);
    cachedRecord.put("id", "cached_id");
    cachedRecord.put("name", "cached_name");
    cachedRecord.put("age", 42);

    // Create RMD with field-level timestamps
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("age", 10L);
    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(userRmdSchemaV1, fieldNameToTimestampMap);

    // Provide old value bytes that would be used on cache miss — use a different value to distinguish
    GenericRecord differentRecord = new GenericData.Record(userSchemaV1);
    differentRecord.put("id", "bytes_id");
    differentRecord.put("name", "bytes_name");
    differentRecord.put("age", 99);
    ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV1).serialize(differentRecord));

    // Call with matching schema (cachedOldValueSchemaId == readerValueSchemaID == 1)
    ValueAndRmd<GenericRecord> result =
        resolver.createOldValueAndRmd(userSchemaV1, 1, 1, Lazy.of(() -> oldValueBytes), rmdRecord, cachedRecord, 1);

    // The returned record should be the exact same object as the cached record (no deserialization)
    Assert.assertSame(result.getValue(), cachedRecord, "Cache hit should reuse the exact cached GenericRecord object");
    Assert.assertEquals(result.getValue().get("id").toString(), "cached_id");
    Assert.assertEquals(result.getValue().get("age"), 42);
  }

  /**
   * Verifies that when the cached record's schema ID does not match the reader schema ID,
   * the method falls back to deserializing from bytes (schema evolution path).
   */
  @Test
  public void testCacheMissWithMismatchingSchemaFallsBackToDeserialization() {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, userSchemaV1)).when(schemaRepository).getValueSchema(storeName, 1);
    StringAnnotatedStoreSchemaCache schemaCache = new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);

    MergeConflictResolver resolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(schemaCache, new RmdSerDe(schemaCache, RMD_VERSION_ID), storeName, true, false);

    // Create a cached GenericRecord that claims to be from schema 2 (mismatching reader schema 1)
    GenericRecord cachedRecord = new GenericData.Record(userSchemaV1);
    cachedRecord.put("id", "cached_id");
    cachedRecord.put("name", "cached_name");
    cachedRecord.put("age", 42);

    // Create RMD with field-level timestamps for V1 schema fields
    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("age", 10L);
    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(userRmdSchemaV1, fieldNameToTimestampMap);

    // Serialize old value in V1 schema
    GenericRecord oldRecord = new GenericData.Record(userSchemaV1);
    oldRecord.put("id", "bytes_id");
    oldRecord.put("name", "bytes_name");
    oldRecord.put("age", 99);
    ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV1).serialize(oldRecord));

    // Call with mismatching schema: cachedOldValueSchemaId=2 != readerValueSchemaID=1
    // reader and writer are both schema 1, so no RMD schema conversion needed
    ValueAndRmd<GenericRecord> result =
        resolver.createOldValueAndRmd(userSchemaV1, 1, 1, Lazy.of(() -> oldValueBytes), rmdRecord, cachedRecord, 2);

    // The returned record should NOT be the cached record (fell back to byte deserialization)
    Assert.assertNotSame(
        result.getValue(),
        cachedRecord,
        "Cache miss should deserialize from bytes, not reuse cached record");
    // Deserialized from bytes should have the values from the byte serialization, not the cached record
    Assert.assertEquals(result.getValue().get("id").toString(), "bytes_id");
    Assert.assertEquals(result.getValue().get("name").toString(), "bytes_name");
    Assert.assertEquals(result.getValue().get("age"), 99);
  }

  /**
   * Verifies that when no cached record is provided (null), deserialization from bytes is used.
   */
  @Test
  public void testNullCachedValueFallsBackToDeserialization() {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, userSchemaV1)).when(schemaRepository).getValueSchema(storeName, 1);
    StringAnnotatedStoreSchemaCache schemaCache = new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);

    MergeConflictResolver resolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(schemaCache, new RmdSerDe(schemaCache, RMD_VERSION_ID), storeName, true, false);

    Map<String, Long> fieldNameToTimestampMap = new HashMap<>();
    fieldNameToTimestampMap.put("id", 10L);
    fieldNameToTimestampMap.put("name", 10L);
    fieldNameToTimestampMap.put("age", 10L);
    GenericRecord rmdRecord = createRmdWithFieldLevelTimestamp(userRmdSchemaV1, fieldNameToTimestampMap);

    GenericRecord oldRecord = new GenericData.Record(userSchemaV1);
    oldRecord.put("id", "from_bytes");
    oldRecord.put("name", "from_bytes_name");
    oldRecord.put("age", 55);
    ByteBuffer oldValueBytes = ByteBuffer.wrap(getSerializer(userSchemaV1).serialize(oldRecord));

    // Call with null cached value
    ValueAndRmd<GenericRecord> result =
        resolver.createOldValueAndRmd(userSchemaV1, 1, 1, Lazy.of(() -> oldValueBytes), rmdRecord, null, -1);

    Assert.assertEquals(result.getValue().get("id").toString(), "from_bytes");
    Assert.assertEquals(result.getValue().get("age"), 55);
  }
}
