package com.linkedin.venice.schema.merge;

import static com.linkedin.venice.schema.merge.AvroCollectionElementComparator.INSTANCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.IndexedHashMap;
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
