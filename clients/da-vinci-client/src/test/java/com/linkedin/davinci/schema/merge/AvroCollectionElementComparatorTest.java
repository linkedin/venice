package com.linkedin.davinci.schema.merge;

import static com.linkedin.davinci.schema.merge.AvroCollectionElementComparator.INSTANCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.utils.IndexedHashMap;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class AvroCollectionElementComparatorTest {
  @Test
  public void testCompareWhenSchemaTypeIsNullShouldReturnZero() {
    assertEquals(INSTANCE.compare(new GenericData(), new GenericData(), Schema.create(Schema.Type.NULL)), 0);
  }

  @Test
  public void testCompareWithStringSchema() {
    assertEquals(INSTANCE.compare("A", "A", Schema.create(Schema.Type.STRING)), 0);
    assertTrue(INSTANCE.compare("A", "B", Schema.create(Schema.Type.STRING)) <= -1);
    assertTrue(INSTANCE.compare("Z", "A", Schema.create(Schema.Type.STRING)) >= 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompareWhenSchemaIsNull() {
    INSTANCE.compare(new GenericData(), new GenericData(), null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Expect IndexedHashMap.*")
  public void testCompareWhenSchemaIsMapAndObjectIsNotIndexedHashMap() {
    INSTANCE.compare(new GenericData(), new GenericData(), Schema.createMap(Schema.create(Schema.Type.LONG)));
  }

  @Test
  public void testCompareWhenSchemaIsNullableUnionPair() {
    Schema.Field testField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("testField").build();
    Schema genericRecordSchema =
        Schema.createRecord("TestRecord", "schema for testing", "com.linkedin.venice.test", false);
    genericRecordSchema.setFields(Collections.singletonList(testField));
    GenericRecord genericRecord = new GenericData.Record(genericRecordSchema);
    genericRecord.put("testField", 10);

    Schema schema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(genericRecordSchema));
    IndexedHashMap<String, GenericRecord> map = new IndexedHashMap<>();
    map.put("k1", genericRecord);
    map.put("k2", genericRecord);
    assertEquals(INSTANCE.compare(map, map, schema), 0);

    GenericRecord genericRecord1 = new GenericData.Record(genericRecordSchema);
    genericRecord1.put("testField", 10);
    IndexedHashMap<String, GenericRecord> map1 = new IndexedHashMap<>();
    map1.put("k1", genericRecord1);
    map1.put("k2", genericRecord1);
    assertEquals(INSTANCE.compare(map, map1, schema), 0);

    genericRecord1.put("testField", 20);
    assertTrue(INSTANCE.compare(map, map1, schema) <= -1);
  }

  @Test
  public void testCompareWhenObjectSchemasAreDifferent() {
    Schema.Field testField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("testField").build();
    Schema genericRecordSchema =
        Schema.createRecord("TestRecord", "schema for testing", "com.linkedin.venice.test", false);
    genericRecordSchema.setFields(Collections.singletonList(testField));
    GenericRecord genericRecord = new GenericData.Record(genericRecordSchema);
    genericRecord.put("testField", 10);
    IndexedHashMap<String, GenericRecord> map = new IndexedHashMap<>();
    map.put("k1", genericRecord);
    map.put("k2", genericRecord);

    Schema.Field testField1 =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.LONG)).setName("testField").build();
    Schema genericRecordSchema1 =
        Schema.createRecord("TestRecord1", "schema for testing", "com.linkedin.venice.test", false);
    genericRecordSchema1.setFields(Collections.singletonList(testField1));
    GenericRecord genericRecord1 = new GenericData.Record(genericRecordSchema1);
    genericRecord1.put("testField", 10L);
    IndexedHashMap<String, GenericRecord> map1 = new IndexedHashMap<>();
    map1.put("k1", genericRecord1);
    map1.put("k2", genericRecord1);

    assertNotEquals(INSTANCE.compare(map, map1, Schema.createMap(genericRecordSchema)), 0);
  }

  @Test
  public void testCompareWhenSchemaIsMapAndObjectsAreTheSameIndexedHashMaps() {
    Schema.Field testField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("testField").build();
    Schema schema = Schema.createRecord("TestRecord", "schema for testing", "com.linkedin.venice.test", false);
    schema.setFields(Collections.singletonList(testField));
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("testField", 10);

    IndexedHashMap<String, GenericRecord> map = new IndexedHashMap<>();
    map.put("k1", genericRecord);
    map.put("k2", genericRecord);
    assertEquals(INSTANCE.compare(map, map, Schema.createMap(schema)), 0);

    IndexedHashMap<String, GenericRecord> map1 = new IndexedHashMap<>();
    map1.put("k1", genericRecord);
    map1.put("k2", genericRecord);
    assertEquals(INSTANCE.compare(map, map1, Schema.createMap(schema)), 0);
  }

  @Test
  public void testCompareRecordWithNestedMapField() {
    // This reproduces the "Can't compare maps!" error from GenericData.compare() when
    // list elements are records containing map fields.
    Schema mapFieldSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema.Field intField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("id").build();
    Schema.Field mapField =
        AvroCompatibilityHelper.newField(null).setSchema(mapFieldSchema).setName("attributes").build();
    Schema recordSchema = Schema.createRecord("RecordWithMap", "test", "com.linkedin.venice.test", false);
    recordSchema.setFields(Arrays.asList(intField, mapField));

    IndexedHashMap<String, Object> map1 = new IndexedHashMap<>();
    map1.put("key1", "value1");
    GenericRecord r1 = new GenericData.Record(recordSchema);
    r1.put("id", 1);
    r1.put("attributes", map1);

    IndexedHashMap<String, Object> map2 = new IndexedHashMap<>();
    map2.put("key1", "value1");
    GenericRecord r2 = new GenericData.Record(recordSchema);
    r2.put("id", 1);
    r2.put("attributes", map2);

    // Same content should be equal
    assertEquals(INSTANCE.compare(r1, r2, recordSchema), 0);

    // Different id field should produce non-zero
    r2.put("id", 2);
    assertTrue(INSTANCE.compare(r1, r2, recordSchema) < 0);

    // Same id, different map content
    r2.put("id", 1);
    IndexedHashMap<String, Object> map3 = new IndexedHashMap<>();
    map3.put("key2", "value2");
    r2.put("attributes", map3);
    assertNotEquals(INSTANCE.compare(r1, r2, recordSchema), 0);
  }

  @Test
  public void testCompareRecordWithNestedNullableMapField() {
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema nullableMapSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), mapSchema);
    Schema.Field nullableMapField =
        AvroCompatibilityHelper.newField(null).setSchema(nullableMapSchema).setName("attributes").build();
    Schema recordSchema = Schema.createRecord("RecordWithNullableMap", "test", "com.linkedin.venice.test", false);
    recordSchema.setFields(Collections.singletonList(nullableMapField));

    IndexedHashMap<String, Object> map1 = new IndexedHashMap<>();
    map1.put("k", "v");
    GenericRecord r1 = new GenericData.Record(recordSchema);
    r1.put("attributes", map1);

    GenericRecord r2 = new GenericData.Record(recordSchema);
    r2.put("attributes", null);

    // Non-null vs null: null branch (index 0) < map branch (index 1)
    assertTrue(INSTANCE.compare(r1, r2, recordSchema) > 0);
  }

  @Test
  public void testCompareMapWithPrimitiveValuesSameKeysDifferentValues() {
    // Regression test: old code would ClassCastException (Integer cannot be cast to GenericContainer)
    // when comparing maps with primitive values that share the same keys.
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k1", 2);
    assertTrue(INSTANCE.compare(map1, map2, Schema.createMap(Schema.create(Schema.Type.INT))) < 0);
    assertTrue(INSTANCE.compare(map2, map1, Schema.createMap(Schema.create(Schema.Type.INT))) > 0);

    // Equal values
    IndexedHashMap<String, Integer> map3 = new IndexedHashMap<>();
    map3.put("k1", 1);
    assertEquals(INSTANCE.compare(map1, map3, Schema.createMap(Schema.create(Schema.Type.INT))), 0);
  }

  @Test
  public void testCompareRecordWithNestedArrayField() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema.Field arrayField = AvroCompatibilityHelper.newField(null).setSchema(arraySchema).setName("tags").build();
    Schema recordSchema = Schema.createRecord("RecordWithArray", "test", "com.linkedin.venice.test", false);
    recordSchema.setFields(Collections.singletonList(arrayField));

    GenericRecord r1 = new GenericData.Record(recordSchema);
    r1.put("tags", Arrays.asList("a", "b"));
    GenericRecord r2 = new GenericData.Record(recordSchema);
    r2.put("tags", Arrays.asList("a", "c"));

    // "b" < "c"
    assertTrue(INSTANCE.compare(r1, r2, recordSchema) < 0);

    // Equal arrays
    GenericRecord r3 = new GenericData.Record(recordSchema);
    r3.put("tags", Arrays.asList("a", "b"));
    assertEquals(INSTANCE.compare(r1, r3, recordSchema), 0);

    // Different lengths: [a, b] vs [a, b, c]
    GenericRecord r4 = new GenericData.Record(recordSchema);
    r4.put("tags", Arrays.asList("a", "b", "c"));
    assertTrue(INSTANCE.compare(r1, r4, recordSchema) < 0);
    assertTrue(INSTANCE.compare(r4, r1, recordSchema) > 0);
  }

  @Test
  public void testCompareMapWithNullableUnionValues() {
    // Regression: when map value schema is a union (e.g. ["null", "record"]) and one entry value is null
    // while the other is a GenericContainer, compareMaps must not cast entry2 unconditionally.
    Schema recordSchema = Schema.createRecord("Inner", "test", "com.linkedin.venice.test", false);
    Schema.Field intField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("val").build();
    recordSchema.setFields(Collections.singletonList(intField));

    Schema nullableRecordSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), recordSchema);
    Schema mapSchema = Schema.createMap(nullableRecordSchema);

    GenericRecord inner = new GenericData.Record(recordSchema);
    inner.put("val", 42);

    // map1 has a non-null value, map2 has a null value for the same key
    IndexedHashMap<String, Object> map1 = new IndexedHashMap<>();
    map1.put("k1", inner);
    IndexedHashMap<String, Object> map2 = new IndexedHashMap<>();
    map2.put("k1", null);

    // null branch (index 0) < record branch (index 1), so map2 < map1
    assertTrue(INSTANCE.compare(map1, map2, mapSchema) > 0);
    assertTrue(INSTANCE.compare(map2, map1, mapSchema) < 0);

    // Both null
    IndexedHashMap<String, Object> map3 = new IndexedHashMap<>();
    map3.put("k1", null);
    assertEquals(INSTANCE.compare(map2, map3, mapSchema), 0);
  }

  @Test
  public void testCompareArrayOfRecordsWithMapFields() {
    // 3-level recursion: ARRAY -> RECORD -> MAP -> primitive
    // This is the real-world scenario: collection list elements are records containing map fields.
    Schema mapFieldSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema.Field idField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("id").build();
    Schema.Field attrField = AvroCompatibilityHelper.newField(null).setSchema(mapFieldSchema).setName("attrs").build();
    Schema elemSchema = Schema.createRecord("Element", "test", "com.linkedin.venice.test", false);
    elemSchema.setFields(Arrays.asList(idField, attrField));
    Schema arraySchema = Schema.createArray(elemSchema);

    IndexedHashMap<String, Object> m1 = new IndexedHashMap<>();
    m1.put("color", "red");
    GenericRecord e1 = new GenericData.Record(elemSchema);
    e1.put("id", 1);
    e1.put("attrs", m1);

    IndexedHashMap<String, Object> m2 = new IndexedHashMap<>();
    m2.put("color", "red");
    GenericRecord e2 = new GenericData.Record(elemSchema);
    e2.put("id", 1);
    e2.put("attrs", m2);

    // Equal arrays of records
    assertEquals(INSTANCE.compare(Arrays.asList(e1), Arrays.asList(e2), arraySchema), 0);

    // Differ at record primitive field
    GenericRecord e3 = new GenericData.Record(elemSchema);
    e3.put("id", 2);
    e3.put("attrs", m2);
    assertTrue(INSTANCE.compare(Arrays.asList(e1), Arrays.asList(e3), arraySchema) < 0);

    // Same id, differ at nested map value
    IndexedHashMap<String, Object> m3 = new IndexedHashMap<>();
    m3.put("color", "blue");
    GenericRecord e4 = new GenericData.Record(elemSchema);
    e4.put("id", 1);
    e4.put("attrs", m3);
    assertNotEquals(INSTANCE.compare(Arrays.asList(e1), Arrays.asList(e4), arraySchema), 0);

    // Multiple elements: first equal, second differs
    assertTrue(INSTANCE.compare(Arrays.asList(e1, e1), Arrays.asList(e1, e3), arraySchema) < 0);
  }

  @Test
  public void testCompareNestedRecords() {
    // 2-level record recursion: RECORD -> RECORD -> primitive
    Schema innerSchema = Schema.createRecord("Inner", "test", "com.linkedin.venice.test", false);
    Schema.Field valField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.INT)).setName("val").build();
    innerSchema.setFields(Collections.singletonList(valField));

    Schema outerSchema = Schema.createRecord("Outer", "test", "com.linkedin.venice.test", false);
    Schema.Field nameField =
        AvroCompatibilityHelper.newField(null).setSchema(Schema.create(Schema.Type.STRING)).setName("name").build();
    Schema.Field nestedField = AvroCompatibilityHelper.newField(null).setSchema(innerSchema).setName("nested").build();
    outerSchema.setFields(Arrays.asList(nameField, nestedField));

    GenericRecord inner1 = new GenericData.Record(innerSchema);
    inner1.put("val", 10);
    GenericRecord outer1 = new GenericData.Record(outerSchema);
    outer1.put("name", "a");
    outer1.put("nested", inner1);

    GenericRecord inner2 = new GenericData.Record(innerSchema);
    inner2.put("val", 10);
    GenericRecord outer2 = new GenericData.Record(outerSchema);
    outer2.put("name", "a");
    outer2.put("nested", inner2);

    // Equal
    assertEquals(INSTANCE.compare(outer1, outer2, outerSchema), 0);

    // Differ at inner record field
    inner2.put("val", 20);
    assertTrue(INSTANCE.compare(outer1, outer2, outerSchema) < 0);

    // Differ at outer field (short-circuits before reaching inner)
    outer2.put("name", "b");
    assertTrue(INSTANCE.compare(outer1, outer2, outerSchema) < 0);
  }

  @Test
  public void testCompareNestedMaps() {
    // MAP -> MAP -> primitive (map whose values are maps)
    Schema innerMapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    Schema outerMapSchema = Schema.createMap(innerMapSchema);

    IndexedHashMap<String, Object> innerMap1 = new IndexedHashMap<>();
    innerMap1.put("x", 1);
    IndexedHashMap<String, Object> outerMap1 = new IndexedHashMap<>();
    outerMap1.put("k", innerMap1);

    IndexedHashMap<String, Object> innerMap2 = new IndexedHashMap<>();
    innerMap2.put("x", 1);
    IndexedHashMap<String, Object> outerMap2 = new IndexedHashMap<>();
    outerMap2.put("k", innerMap2);

    // Equal
    assertEquals(INSTANCE.compare(outerMap1, outerMap2, outerMapSchema), 0);

    // Differ at inner map value
    IndexedHashMap<String, Object> innerMap3 = new IndexedHashMap<>();
    innerMap3.put("x", 2);
    IndexedHashMap<String, Object> outerMap3 = new IndexedHashMap<>();
    outerMap3.put("k", innerMap3);
    assertTrue(INSTANCE.compare(outerMap1, outerMap3, outerMapSchema) < 0);
  }

  @Test
  public void testCompareWhenSchemaIsMapAndObjectsAreIndexedHashMaps() {
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k3", 3);

    // map1's size is greater than map2's size
    assertTrue(INSTANCE.compare(map1, map2, Schema.createMap(Schema.create(Schema.Type.INT))) >= 1);

    map2.put("k4", 4);
    // map1 and map2's size is the same but values differ. map1's first key is smaller than the map2's first key
    assertTrue(INSTANCE.compare(map1, map2, Schema.createMap(Schema.create(Schema.Type.INT))) <= -1);

    map2.put("k5", 5);
    // map2's size is greater than map1's size
    assertTrue(INSTANCE.compare(map1, map2, Schema.createMap(Schema.create(Schema.Type.INT))) <= -1);
  }
}
