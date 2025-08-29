package com.linkedin.davinci.compression;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * This class provides functionality to compress and decompress keys containing URNs using a URN dictionary.
 * It supports both string type key schema (when the entire key is a URN) and record type key schema (when the key
 * is a record containing one or more URN fields).
 */
public class KeyUrnCompressor {
  /**
   * Create a schema with a record type, which contains a single field named "urn_type_id" of type int and
   * a single field named "urn_remainder" of type string.
   */
  private static final Schema URN_SCHEMA_WITH_COMPRESSION = Schema.createRecord(
      "CompressedKey",
      null,
      null,
      false,
      Arrays.asList(
          AvroCompatibilityHelper.createSchemaField("urn_type_id", Schema.create(Schema.Type.INT), null, null),
          AvroCompatibilityHelper.createSchemaField("urn_remainder", Schema.create(Schema.Type.STRING), null, null)));

  private final Schema keySchema;
  private final List<String> urnFieldNames;
  private final Set<String> urnFieldNamesSet;
  private final UrnDictV1 urnDict;
  private final Schema keySchemaWithCompression;

  private final RecordSerializer<Object> keySerializer;
  private final RecordDeserializer<Object> keyDeserializer;
  private final RecordSerializer<Object> keyWithCompressionSerializer;
  private final RecordDeserializer<Object> keyWithCompressionDeserializer;

  public KeyUrnCompressor(Schema keySchema, UrnDictV1 urnDict) {
    this(keySchema, Collections.emptyList(), urnDict);
  }

  public KeyUrnCompressor(Schema keySchema, List<String> urnFieldNames, UrnDictV1 urnDict) {
    validateKeySchemaBasedOnUrnFieldNames(keySchema, urnFieldNames);

    this.keySchema = keySchema;
    this.urnFieldNames = urnFieldNames;
    this.urnFieldNamesSet = Collections.unmodifiableSet(new HashSet<>(urnFieldNames));
    this.urnDict = urnDict;
    this.keySchemaWithCompression = generateKeySchemaWithCompression(keySchema, urnFieldNames);
    this.keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    this.keyWithCompressionSerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchemaWithCompression);
    this.keyWithCompressionDeserializer = FastSerializerDeserializerFactory
        .getFastAvroGenericDeserializer(keySchemaWithCompression, keySchemaWithCompression);
  }

  public byte[] compressKey(byte[] key, boolean updateDictUponUnknownUrn) {
    return compressKey(keyDeserializer.deserialize(key), updateDictUponUnknownUrn);
  }

  public byte[] compressKey(Object keyObject, boolean updateDictUponUnknownUrn) {
    if (keySchema.getType().equals(Schema.Type.STRING)) {
      UrnDictV1.EncodedUrn encodedUrn = urnDict.encodeUrn(keyObject.toString(), updateDictUponUnknownUrn);
      GenericRecord compressedKey = new GenericData.Record(keySchemaWithCompression);
      compressedKey.put("urn_type_id", encodedUrn.urnTypeId);
      compressedKey.put("urn_remainder", encodedUrn.urnRemainder);
      return keyWithCompressionSerializer.serialize(compressedKey);
    }
    // Record type key schema
    if (!(keyObject instanceof GenericRecord)) {
      throw new IllegalArgumentException(
          "Expected key object to be a GenericRecord, but found: " + keyObject.getClass());
    }
    GenericRecord keyRecord = (GenericRecord) keyObject;
    GenericRecord compressedKey = new GenericData.Record(keySchemaWithCompression);
    for (Schema.Field field: keySchema.getFields()) {
      if (urnFieldNamesSet.contains(field.name())) {
        String urnValue = keyRecord.get(field.name()).toString();
        UrnDictV1.EncodedUrn encodedUrn = urnDict.encodeUrn(urnValue, updateDictUponUnknownUrn);
        GenericRecord compressedUrn = new GenericData.Record(URN_SCHEMA_WITH_COMPRESSION);
        compressedUrn.put("urn_type_id", encodedUrn.urnTypeId);
        compressedUrn.put("urn_remainder", encodedUrn.urnRemainder);
        compressedKey.put(field.name(), compressedUrn);
      } else {
        // For non-urn fields, keep the original value
        compressedKey.put(field.name(), keyRecord.get(field.name()));
      }
    }
    return keyWithCompressionSerializer.serialize(compressedKey);
  }

  public Object decompressAndDecodeKey(byte[] compressedKey) {
    GenericRecord compressedKeyRecord = (GenericRecord) keyWithCompressionDeserializer.deserialize(compressedKey);
    if (keySchema.getType().equals(Schema.Type.STRING)) {
      // For string type key schema, decode the compressed key directly
      int urnTypeId = (int) compressedKeyRecord.get("urn_type_id");
      String urnRemainder = compressedKeyRecord.get("urn_remainder").toString();
      return urnDict.decodeUrn(new UrnDictV1.EncodedUrn(urnTypeId, urnRemainder));
    }
    // Record type key schema
    GenericRecord decompressedKey = new GenericData.Record(keySchema);
    for (Schema.Field field: keySchemaWithCompression.getFields()) {
      if (urnFieldNamesSet.contains(field.name())) {
        GenericRecord compressedUrn = (GenericRecord) compressedKeyRecord.get(field.name());
        int urnTypeId = (int) compressedUrn.get("urn_type_id");
        String urnRemainder = compressedUrn.get("urn_remainder").toString();
        decompressedKey.put(field.name(), urnDict.decodeUrn(new UrnDictV1.EncodedUrn(urnTypeId, urnRemainder)));
      } else {
        // For non-urn fields, keep the original value
        decompressedKey.put(field.name(), compressedKeyRecord.get(field.name()));
      }
    }
    return decompressedKey;
  }

  public byte[] decompressKey(byte[] compressedKey) {
    return keySerializer.serialize(decompressAndDecodeKey(compressedKey));
  }

  public static void validateKeySchemaBasedOnUrnFieldNames(Schema keySchema, List<String> urnFieldNames) {
    if (urnFieldNames.isEmpty()) {
      // When no urn fields are specified, the schema must be a string schema.
      if (!keySchema.getType().equals(Schema.Type.STRING)) {
        throw new IllegalArgumentException(
            "When no urn fields are specified, the key schema must be a string schema. Found: " + keySchema);
      }
    } else {
      // When urn fields are specified, the schema must be a record schema and must contain all the specified urn
      // fields.
      if (!keySchema.getType().equals(Schema.Type.RECORD)) {
        throw new IllegalArgumentException(
            "When urn fields are specified, the key schema must be a record schema. Found: " + keySchema);
      }
      for (String urnFieldName: urnFieldNames) {
        if (keySchema.getField(urnFieldName) == null) {
          throw new IllegalArgumentException(
              "The key schema does not contain the specified urn field: " + urnFieldName);
        }
        if (!keySchema.getField(urnFieldName).schema().getType().equals(Schema.Type.STRING)) {
          throw new IllegalArgumentException(
              "The specified urn field must be of string type. Found: " + keySchema.getField(urnFieldName).schema());
        }
      }
    }
  }

  static Schema generateKeySchemaWithCompression(Schema keySchema, List<String> urnFieldNames) {
    validateKeySchemaBasedOnUrnFieldNames(keySchema, urnFieldNames);
    if (urnFieldNames.isEmpty()) {
      return URN_SCHEMA_WITH_COMPRESSION;
    } else {
      /**
       * For record type key schema, create a new record schema with the same name and namespace as the original schema.
       * For the urn fields, replace their types with the URN_SCHEMA_WITH_COMPRESSION schema.
       * For the non-urn fields, keep their original types.
       */
      List<Schema.Field> newFields = new ArrayList<>();
      for (Schema.Field field: keySchema.getFields()) {
        if (urnFieldNames.contains(field.name())) {
          newFields.add(
              AvroCompatibilityHelper.createSchemaField(
                  field.name(),
                  URN_SCHEMA_WITH_COMPRESSION,
                  field.doc(),
                  field.hasDefaultValue() ? AvroCompatibilityHelper.getGenericDefaultValue(field) : null));
        } else {
          newFields.add(
              AvroCompatibilityHelper.createSchemaField(
                  field.name(),
                  field.schema(),
                  field.doc(),
                  field.hasDefaultValue() ? AvroCompatibilityHelper.getGenericDefaultValue(field) : null));
        }
      }
      return Schema.createRecord(keySchema.getName(), keySchema.getDoc(), keySchema.getNamespace(), false, newFields);
    }
  }

  public UrnDictV1 getUrnDict() {
    return urnDict;
  }

  public List<String> getUrnFieldNames() {
    return urnFieldNames;
  }
}
