package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.PUT_ONLY_PART_LENGTH_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_COLO_ID_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.UNION;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchemaUtils {
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadSchemaFileAsString("testMergeSchema.avsc"));
  private static final String LIST_FIELD_NAME = "StringListField";
  private static final String MAP_FIELD_NAME = "IntMapField";
  private static final String NULLABLE_LIST_FIELD_NAME = "NullableStringListField";
  private static final String NULLABLE_MAP_FIELD_NAME = "NullableIntMapField";

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testAnnotateValueSchema(boolean useStrictValidation) {
    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put(LIST_FIELD_NAME, Collections.singletonList("key1"));
    Map<String, Integer> integerMap = new HashMap<>();
    integerMap.put("key1", 1);
    valueRecord.put(MAP_FIELD_NAME, integerMap);

    byte[] serializedBytes = getSerializer(VALUE_SCHEMA).serialize(valueRecord);
    Schema annotatedValueSchema = SchemaUtils.annotateValueSchema(VALUE_SCHEMA, useStrictValidation);
    GenericRecord deserializedValueRecord =
        getDeserializer(annotatedValueSchema, annotatedValueSchema).deserialize(serializedBytes);
    Assert.assertEquals(((List<String>) deserializedValueRecord.get(LIST_FIELD_NAME)).get(0), "key1");
    Assert.assertEquals(((Map<String, Integer>) deserializedValueRecord.get(MAP_FIELD_NAME)).get("key1"), (Integer) 1);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testAnnotateUpdateSchema(boolean useStrictValidation) {
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);
    Schema annotatedUpdateSchema = SchemaUtils.annotateUpdateSchema(updateSchema, useStrictValidation);

    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    GenericRecord updateRecord =
        updateBuilder.setElementsToAddToListField(LIST_FIELD_NAME, Collections.singletonList("key1"))
            .setElementsToRemoveFromListField(LIST_FIELD_NAME, Collections.singletonList("key2"))
            .setEntriesToAddToMapField(MAP_FIELD_NAME, Collections.singletonMap("key3", 1))
            .setKeysToRemoveFromMapField(MAP_FIELD_NAME, Collections.singletonList("key4"))
            .build();
    byte[] serializedBytes = getSerializer(updateSchema).serialize(updateRecord);
    GenericRecord deserializedValueRecord =
        getDeserializer(annotatedUpdateSchema, annotatedUpdateSchema).deserialize(serializedBytes);
    GenericRecord listOps = (GenericRecord) deserializedValueRecord.get(LIST_FIELD_NAME);
    Assert.assertEquals(listOps.get(SET_UNION), Collections.singletonList("key1"));
    Assert.assertEquals(listOps.get(SET_DIFF), Collections.singletonList("key2"));
    GenericRecord mapOps = (GenericRecord) deserializedValueRecord.get(MAP_FIELD_NAME);
    Assert.assertEquals(mapOps.get(MAP_UNION), Collections.singletonMap("key3", 1));
    Assert.assertEquals(mapOps.get(MAP_DIFF), Collections.singletonList("key4"));

    updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateRecord = updateBuilder.setNewFieldValue(LIST_FIELD_NAME, Collections.singletonList("key1"))
        .setNewFieldValue(MAP_FIELD_NAME, Collections.singletonMap("key2", 1))
        .build();
    serializedBytes = getSerializer(updateSchema).serialize(updateRecord);
    deserializedValueRecord =
        getDeserializer(annotatedUpdateSchema, annotatedUpdateSchema).deserialize(serializedBytes);
    Assert.assertEquals(deserializedValueRecord.get(LIST_FIELD_NAME), Collections.singletonList("key1"));
    Assert.assertEquals(deserializedValueRecord.get(MAP_FIELD_NAME), Collections.singletonMap("key2", 1));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testAnnotateRmdSchema(boolean useStrictValidation) {
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA);
    Schema annotatedRmdSchema = SchemaUtils.annotateRmdSchema(rmdSchema, useStrictValidation);
    GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    Schema tsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema listFieldTsSchema = tsSchema.getField(LIST_FIELD_NAME).schema();
    Schema mapFieldTsSchema = tsSchema.getField(MAP_FIELD_NAME).schema();
    Schema nullableListFieldTsSchema = tsSchema.getField(NULLABLE_LIST_FIELD_NAME).schema();
    Schema nullableMapFieldTsSchema = tsSchema.getField(NULLABLE_MAP_FIELD_NAME).schema();

    GenericRecord listFieldTsRecord = new GenericData.Record(listFieldTsSchema);
    listFieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    listFieldTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    listFieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    listFieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L, 4L));
    listFieldTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.singletonList("key1"));
    listFieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.singletonList(5L));

    GenericRecord mapFieldTsRecord = new GenericData.Record(mapFieldTsSchema);
    mapFieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapFieldTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    mapFieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 1);
    mapFieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Arrays.asList(2L, 3L, 4L));
    mapFieldTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.singletonList("key2"));
    mapFieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.singletonList(5L));

    GenericRecord nullableListFieldTsRecord = new GenericData.Record(nullableListFieldTsSchema);
    nullableListFieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    nullableListFieldTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    nullableListFieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    nullableListFieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.emptyList());
    nullableListFieldTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.emptyList());
    nullableListFieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.emptyList());

    GenericRecord nullableMapFieldTsRecord = new GenericData.Record(nullableMapFieldTsSchema);
    nullableMapFieldTsRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    nullableMapFieldTsRecord.put(TOP_LEVEL_COLO_ID_FIELD_NAME, 0);
    nullableMapFieldTsRecord.put(PUT_ONLY_PART_LENGTH_FIELD_NAME, 0);
    nullableMapFieldTsRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.emptyList());
    nullableMapFieldTsRecord.put(DELETED_ELEM_FIELD_NAME, Collections.emptyList());
    nullableMapFieldTsRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.emptyList());

    GenericRecord tsRecord = new GenericData.Record(tsSchema);
    tsRecord.put(LIST_FIELD_NAME, listFieldTsRecord);
    tsRecord.put(MAP_FIELD_NAME, mapFieldTsRecord);
    tsRecord.put(NULLABLE_LIST_FIELD_NAME, nullableListFieldTsRecord);
    tsRecord.put(NULLABLE_MAP_FIELD_NAME, nullableMapFieldTsRecord);
    rmdRecord.put(TIMESTAMP_FIELD_NAME, tsRecord);
    rmdRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, Collections.emptyList());
    byte[] serializedBytes = getSerializer(rmdSchema).serialize(rmdRecord);
    GenericRecord deserializedValueRecord =
        getDeserializer(annotatedRmdSchema, annotatedRmdSchema).deserialize(serializedBytes);
    listFieldTsRecord =
        (GenericRecord) ((GenericRecord) deserializedValueRecord.get(TIMESTAMP_FIELD_NAME)).get(LIST_FIELD_NAME);
    mapFieldTsRecord =
        (GenericRecord) ((GenericRecord) deserializedValueRecord.get(TIMESTAMP_FIELD_NAME)).get(MAP_FIELD_NAME);
    Assert.assertEquals(listFieldTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.singletonList("key1"));
    Assert.assertEquals(mapFieldTsRecord.get(DELETED_ELEM_FIELD_NAME), Collections.singletonList("key2"));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "All-Avro-Schemas-Except-Null-And-Union")
  public void testUnwrapOptionalUnion(Schema type) {
    Schema nullAndTypeUnion = Schema.createUnion(Schema.create(NULL), type);
    assertTrue(SchemaUtils.unwrapOptionalUnion(nullAndTypeUnion) == type.getType());

    Schema typeAndNullUnion = Schema.createUnion(type, Schema.create(NULL));
    assertTrue(SchemaUtils.unwrapOptionalUnion(typeAndNullUnion) == type.getType());

    Schema singleTypeOnlyUnion = Schema.createUnion(type);
    assertTrue(SchemaUtils.unwrapOptionalUnion(singleTypeOnlyUnion) == type.getType());

    Schema other = Schema.create(type.getType() == INT ? LONG : INT);
    Schema typeAndOtherTypeUnion = Schema.createUnion(type, other);
    assertTrue(SchemaUtils.unwrapOptionalUnion(typeAndOtherTypeUnion) == UNION);

    Schema nullAndTypeAndOtherTypeUnion = Schema.createUnion(Schema.create(NULL), type, other);
    assertTrue(SchemaUtils.unwrapOptionalUnion(nullAndTypeAndOtherTypeUnion) == UNION);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "All-Avro-Schemas-Except-Null-And-Union")
  public void testValidateFieldSchemaType(Schema type) {
    Schema nullAndTypeUnion = Schema.createUnion(Schema.create(NULL), type);
    SchemaUtils.validateFieldSchemaType("field", nullAndTypeUnion, type.getType());

    Schema typeAndNullUnion = Schema.createUnion(type, Schema.create(NULL));
    SchemaUtils.validateFieldSchemaType("field", typeAndNullUnion, type.getType());

    Schema singleTypeOnlyUnion = Schema.createUnion(type);
    SchemaUtils.validateFieldSchemaType("field", singleTypeOnlyUnion, type.getType());

    Schema other = Schema.create(type.getType() == INT ? LONG : INT);
    Schema typeAndOtherTypeUnion = Schema.createUnion(type, other);
    Assert.assertThrows(() -> SchemaUtils.validateFieldSchemaType("field", typeAndOtherTypeUnion, type.getType()));

    Schema nullAndTypeAndOtherTypeUnion = Schema.createUnion(Schema.create(NULL), type, other);
    Assert
        .assertThrows(() -> SchemaUtils.validateFieldSchemaType("field", nullAndTypeAndOtherTypeUnion, type.getType()));
  }

  protected RecordSerializer<GenericRecord> getSerializer(Schema writerSchema) {
    return MapOrderPreservingSerDeFactory.getAvroGenericSerializer(writerSchema);
  }

  protected RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return MapOrderPreservingSerDeFactory.getAvroGenericDeserializer(writerSchema, readerSchema);
  }

  private static String loadSchemaFileAsString(String filePath) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }
}
