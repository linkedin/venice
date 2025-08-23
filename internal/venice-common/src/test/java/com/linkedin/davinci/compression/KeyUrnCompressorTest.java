package com.linkedin.davinci.compression;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KeyUrnCompressorTest {
  private UrnDictV1 urnDict;

  @BeforeClass
  public void setUp() {
    List<String> urns = Arrays.asList("urn:li:user:123", "urn:li:group:456", "urn:li:company:789");
    urnDict = UrnDictV1.trainDict(urns, 3);
  }

  @Test
  public void testValidateKeySchema() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema recordSchema = Schema.createRecord(
        "TestRecord",
        null,
        null,
        false,
        Collections.singletonList(
            AvroCompatibilityHelper.createSchemaField("urn_field", Schema.create(Schema.Type.STRING), null, null)));

    // Valid cases
    KeyUrnCompressor.validateKeySchemaBasedOnUrnFieldNames(stringSchema, Collections.emptyList());
    KeyUrnCompressor.validateKeySchemaBasedOnUrnFieldNames(recordSchema, Collections.singletonList("urn_field"));

    // Invalid cases
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KeyUrnCompressor.validateKeySchemaBasedOnUrnFieldNames(recordSchema, Collections.emptyList()));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KeyUrnCompressor
            .validateKeySchemaBasedOnUrnFieldNames(stringSchema, Collections.singletonList("urn_field")));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KeyUrnCompressor
            .validateKeySchemaBasedOnUrnFieldNames(recordSchema, Collections.singletonList("non_existent_field")));

    Schema recordSchemaWithIntField = Schema.createRecord(
        "TestRecord2",
        null,
        null,
        false,
        Collections.singletonList(
            AvroCompatibilityHelper.createSchemaField("urn_field", Schema.create(Schema.Type.INT), null, null)));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> KeyUrnCompressor
            .validateKeySchemaBasedOnUrnFieldNames(recordSchemaWithIntField, Collections.singletonList("urn_field")));
  }

  @Test
  public void testGenerateKeySchemaWithCompression() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema compressedStringSchema =
        KeyUrnCompressor.generateKeySchemaWithCompression(stringSchema, Collections.emptyList());
    Assert.assertEquals(compressedStringSchema.getName(), "CompressedKey");

    Schema recordSchema = Schema.createRecord(
        "TestRecord",
        "doc",
        "ns",
        false,
        Arrays.asList(
            AvroCompatibilityHelper.createSchemaField("urn_field1", Schema.create(Schema.Type.STRING), null, null),
            AvroCompatibilityHelper.createSchemaField("id", Schema.create(Schema.Type.INT), null, null),
            AvroCompatibilityHelper.createSchemaField("urn_field2", Schema.create(Schema.Type.STRING), null, null)));

    Schema compressedSchema =
        KeyUrnCompressor.generateKeySchemaWithCompression(recordSchema, Arrays.asList("urn_field1", "urn_field2"));
    Assert.assertEquals(compressedSchema.getName(), "TestRecord");
    Assert.assertEquals(compressedSchema.getDoc(), "doc");
    Assert.assertEquals(compressedSchema.getNamespace(), "ns");
    Assert.assertEquals(compressedSchema.getFields().size(), 3);
    Assert.assertEquals(compressedSchema.getField("id").schema().getType(), Schema.Type.INT);
    Assert.assertEquals(compressedSchema.getField("urn_field1").schema().getName(), "CompressedKey");
    Assert.assertEquals(compressedSchema.getField("urn_field2").schema().getName(), "CompressedKey");
  }

  @Test
  public void testCompressDecompressStringKey() {
    Schema keySchema = Schema.create(Schema.Type.STRING);
    RecordSerializer<Object> keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    KeyUrnCompressor keyUrnCompressor = new KeyUrnCompressor(keySchema, urnDict);

    String originalKey = "urn:li:user:999";
    byte[] compressed = keyUrnCompressor.compressKey(originalKey, true);
    byte[] decompressed = keyUrnCompressor.decompressKey(compressed);

    Assert.assertEquals(decompressed, keySerializer.serialize(originalKey));

    Object decompressedObject = keyUrnCompressor.decompressAndDecodeKey(compressed);
    Assert.assertEquals(decompressedObject.toString(), originalKey);
  }

  @Test
  public void testCompressDecompressRecordKey() {
    Schema recordSchema = Schema.createRecord(
        "TestRecord",
        null,
        null,
        false,
        Arrays.asList(
            AvroCompatibilityHelper.createSchemaField("urn_field", Schema.create(Schema.Type.STRING), null, null),
            AvroCompatibilityHelper.createSchemaField("id", Schema.create(Schema.Type.INT), null, null)));

    KeyUrnCompressor keyUrnCompressor =
        new KeyUrnCompressor(recordSchema, Collections.singletonList("urn_field"), urnDict);

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("urn_field", "urn:li:group:abc");
    originalRecord.put("id", 123);

    byte[] compressed = keyUrnCompressor.compressKey(originalRecord, true);
    Object decompressedObject = keyUrnCompressor.decompressAndDecodeKey(compressed);

    Assert.assertTrue(decompressedObject instanceof GenericRecord);
    GenericRecord decompressedRecord = (GenericRecord) decompressedObject;
    Assert.assertEquals(decompressedRecord.get("urn_field").toString(), "urn:li:group:abc");
    Assert.assertEquals(decompressedRecord.get("id"), 123);
  }
}
