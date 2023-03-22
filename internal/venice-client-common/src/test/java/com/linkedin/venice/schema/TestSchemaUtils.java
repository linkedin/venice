package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
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

import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.avro.MapOrderingPreservingSerDeFactory;
import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchemaUtils {
  private static final Schema VALUE_SCHEMA =
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadSchemaFileAsString("testMergeSchema.avsc"));
  private static final String LIST_FIELD_NAME = "StringListField";
  private static final String MAP_FIELD_NAME = "IntMapField";
  private static final String NULLABLE_LIST_FIELD_NAME = "NullableStringListField";
  private static final String NULLABLE_MAP_FIELD_NAME = "NullableIntMapField";

  @Test
  public void testAnnotateValueSchema() {
    GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA);
    valueRecord.put(LIST_FIELD_NAME, Collections.singletonList("key1"));
    Map<String, Integer> integerMap = new IndexedHashMap<>();
    integerMap.put("key1", 1);
    valueRecord.put(MAP_FIELD_NAME, integerMap);

    byte[] serializedBytes = getSerializer(VALUE_SCHEMA).serialize(valueRecord);
    Schema annotatedValueSchema = SchemaUtils.annotateValueSchema(VALUE_SCHEMA);
    GenericRecord deserializedValueRecord =
        getDeserializer(annotatedValueSchema, annotatedValueSchema).deserialize(serializedBytes);
    Assert.assertEquals(((List<String>) deserializedValueRecord.get(LIST_FIELD_NAME)).get(0), "key1");
    Assert.assertEquals(((Map<String, Integer>) deserializedValueRecord.get(MAP_FIELD_NAME)).get("key1"), (Integer) 1);
  }

  @Test
  public void testAnnotateUpdateSchema() {
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);
    Schema annotatedUpdateSchema = SchemaUtils.annotateUpdateSchema(updateSchema);

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

  @Test
  public void testAnnotateRmdSchema() {
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA);
    Schema annotatedRmdSchema = SchemaUtils.annotateRmdSchema(rmdSchema);
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
    rmdRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, Collections.emptyList());
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

  protected RecordSerializer<GenericRecord> getSerializer(Schema writerSchema) {
    return MapOrderingPreservingSerDeFactory.getAvroGenericSerializer(writerSchema);
  }

  protected RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return MapOrderingPreservingSerDeFactory.getAvroGenericDeserializer(writerSchema, readerSchema);
  }

}
