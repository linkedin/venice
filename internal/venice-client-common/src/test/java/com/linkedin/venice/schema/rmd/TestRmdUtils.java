package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import com.linkedin.venice.utils.TestWriteUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestRmdUtils {
  private static final String VALUE_RECORD_SCHEMA_STR =
      "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"User\",\n" + "  \"namespace\" : \"example.avro\",\n"
          + "  \"fields\" : [ {\n" + "    \"name\" : \"id\",\n" + "    \"type\" : \"string\",\n"
          + "    \"default\" : \"default_id\"\n" + "  }, {\n" + "    \"name\" : \"name\",\n"
          + "    \"type\" : \"string\",\n" + "    \"default\" : \"default_name\"\n" + "  }, {\n"
          + "    \"name\" : \"age\",\n" + "    \"type\" : \"int\",\n" + "    \"default\" : -1\n" + "  } ]\n" + "}";

  private Schema valueSchema;
  private Schema rmdSchema;
  private GenericRecord rmdRecordWithValueLevelTimeStamp;
  private GenericRecord rmdRecordWithPerFieldLevelTimeStamp;
  private GenericRecord rmdRecordWithValidPerFieldLevelTimestamp;
  private GenericRecord rmdRecordWithOnlyRootLevelTimestamp;

  @BeforeClass
  public void setUp() {
    valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);
  }

  @BeforeMethod
  public void setRmdRecord() {
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    rmdRecordWithValueLevelTimeStamp = new GenericData.Record(rmdSchema);
    rmdRecordWithValueLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, 20L);
    rmdRecordWithValueLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, vectors);

    rmdRecordWithPerFieldLevelTimeStamp = new GenericData.Record(rmdSchema);
    // This is not a valid value for PER_FIELD_TIMESTAMP type. Use this for testing purpose only.
    rmdRecordWithPerFieldLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, new GenericData.Record(valueSchema));
    rmdRecordWithPerFieldLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, vectors);

    // This one is actually valid (TODO, refactor the rest of the tests to use this)
    RmdSchemaGeneratorV1 rmdSchemaGeneratorV1 = new RmdSchemaGeneratorV1();
    Schema timestampSchema = rmdSchemaGeneratorV1.generateMetadataSchema(TestWriteUtils.USER_WITH_STRING_MAP_SCHEMA);
    Schema mapFieldSchema = timestampSchema.getField("timestamp").schema().getTypes().get(1).getField("value").schema();

    GenericRecord mapFieldRecord = new GenericData.Record(mapFieldSchema);
    long[] activeElemTs = { 10L, 20L };
    long[] deletedElemTs = { 5L };
    mapFieldRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, activeElemTs);
    mapFieldRecord.put(TOP_LEVEL_TS_FIELD_NAME, 1L);
    mapFieldRecord.put(DELETED_ELEM_TS_FIELD_NAME, deletedElemTs);

    GenericRecord timestampRecord =
        new GenericData.Record(timestampSchema.getField("timestamp").schema().getTypes().get(1));
    timestampRecord.put("key", 10L);
    timestampRecord.put("age", 30L);
    timestampRecord.put("value", mapFieldRecord);

    rmdRecordWithValidPerFieldLevelTimestamp = new GenericData.Record(timestampSchema);
    rmdRecordWithOnlyRootLevelTimestamp = new GenericData.Record(timestampSchema);
    rmdRecordWithValidPerFieldLevelTimestamp.put(TIMESTAMP_FIELD_NAME, timestampRecord);
    rmdRecordWithOnlyRootLevelTimestamp.put(TIMESTAMP_FIELD_NAME, 0L);
  }

  @Test
  public void testGetRmdTimestampType() {
    Assert.assertEquals(
        RmdUtils.getRmdTimestampType(rmdRecordWithValueLevelTimeStamp.get(TIMESTAMP_FIELD_NAME)).name(),
        RmdTimestampType.VALUE_LEVEL_TIMESTAMP.name());

    Assert.assertEquals(
        RmdUtils.getRmdTimestampType(rmdRecordWithPerFieldLevelTimeStamp.get(TIMESTAMP_FIELD_NAME)).name(),
        RmdTimestampType.PER_FIELD_TIMESTAMP.name());
  }

  @Test
  public void testHasOffsetAdvanced() {
    List<Long> list1 = new ArrayList<>();
    list1.add(1L);
    list1.add(10L);

    List<Long> list2 = new ArrayList<>();
    list2.add(1L);
    list2.add(9L);

    List<Long> list3 = new ArrayList<>();
    list3.add(10L);

    List<Long> list4 = new ArrayList<>();
    list4.add(-1L);

    Assert.assertFalse(RmdUtils.hasOffsetAdvanced(list1, list2));
    Assert.assertFalse(RmdUtils.hasOffsetAdvanced(list1, Collections.emptyList()));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(list1, list3));
    Assert.assertFalse(RmdUtils.hasOffsetAdvanced(list3, list1));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(list2, list1));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(Collections.emptyList(), list2));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(Collections.emptyList(), Collections.emptyList()));

    List<Long> mergedList = RmdUtils.mergeOffsetVectors(list2, list1);
    Assert.assertEquals(mergedList.get(0), list2.get(0));
    Assert.assertEquals(mergedList.get(1), list1.get(1));

    mergedList = RmdUtils.mergeOffsetVectors(list2, Collections.EMPTY_LIST);
    Assert.assertEquals(mergedList.get(0), list2.get(0));
    Assert.assertEquals(mergedList.get(1), list2.get(1));

    mergedList = RmdUtils.mergeOffsetVectors(Collections.EMPTY_LIST, list2);
    Assert.assertEquals(mergedList.get(0), list2.get(0));
    Assert.assertEquals(mergedList.get(1), list2.get(1));

    mergedList = RmdUtils.mergeOffsetVectors(list1, list3);
    Assert.assertEquals(mergedList.get(0), list3.get(0));
    Assert.assertEquals(mergedList.get(1), list1.get(1));

    mergedList = RmdUtils.mergeOffsetVectors(list3, list1);
    Assert.assertEquals(mergedList.get(0), list3.get(0));
    Assert.assertEquals(mergedList.get(1), list1.get(1));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetUnsupportedRmdTimestampType() {
    GenericRecord dummy = new GenericData.Record(rmdSchema);
    dummy.put(TIMESTAMP_FIELD_NAME, "invalid");
    RmdUtils.getRmdTimestampType(dummy.get(TIMESTAMP_FIELD_NAME));
  }

  @Test
  public void testExtractTimestampFromRmd() {
    List<Long> valueFieldTimeStamp = RmdUtils.extractTimestampFromRmd(rmdRecordWithValueLevelTimeStamp);
    Assert.assertEquals(1, valueFieldTimeStamp.size());
    Assert.assertEquals(20, valueFieldTimeStamp.get(0).intValue());

    List<Long> perFieldTimeStamp = RmdUtils.extractTimestampFromRmd(rmdRecordWithPerFieldLevelTimeStamp);
    Assert.assertEquals(1, perFieldTimeStamp.size());
    Assert.assertEquals(0, perFieldTimeStamp.get(0).intValue()); // not supported yet so just return 0
  }

  @Test
  public void testExtractOffsetVectorSumFromRmd() {
    Assert.assertEquals(6, RmdUtils.extractOffsetVectorSumFromRmd(rmdRecordWithValueLevelTimeStamp));
  }

  @Test
  public void testExtractOffsetVectorFromRmd() {
    List<Long> vector = RmdUtils.extractOffsetVectorFromRmd(rmdRecordWithValueLevelTimeStamp);
    Assert.assertEquals(vector, Arrays.asList(1L, 2L, 3L));
    GenericRecord nullRmdRecord = new GenericData.Record(rmdSchema);
    Assert.assertEquals(RmdUtils.extractOffsetVectorFromRmd(nullRmdRecord), Collections.emptyList());
  }

  @Test
  public void testExtractLatestTimestampFromRmd() {
    Assert.assertEquals(RmdUtils.getLastUpdateTimestamp(rmdRecordWithValidPerFieldLevelTimestamp), 30L);
    Assert.assertEquals(RmdUtils.getLastUpdateTimestamp(rmdRecordWithOnlyRootLevelTimestamp), 0L);
  }

  @Test
  public void testGetLastUpdateTimestampWithCollectionFields() {
    RmdSchemaGeneratorV1 rmdSchemaGeneratorV1 = new RmdSchemaGeneratorV1();
    Schema timestampSchema = rmdSchemaGeneratorV1.generateMetadataSchema(TestWriteUtils.USER_WITH_STRING_MAP_SCHEMA);
    Schema mapFieldSchema = timestampSchema.getField("timestamp").schema().getTypes().get(1).getField("value").schema();

    GenericRecord mapFieldRecord = new GenericData.Record(mapFieldSchema);
    long[] activeElemTs = { 100L, 200L, 150L };
    long[] deletedElemTs = { 50L, 75L, 300L }; // 300L should be the max
    mapFieldRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, activeElemTs);
    mapFieldRecord.put(TOP_LEVEL_TS_FIELD_NAME, 25L);
    mapFieldRecord.put(DELETED_ELEM_TS_FIELD_NAME, deletedElemTs);

    GenericRecord timestampRecord =
        new GenericData.Record(timestampSchema.getField("timestamp").schema().getTypes().get(1));
    timestampRecord.put("key", 10L);
    timestampRecord.put("age", 50L);
    timestampRecord.put("value", mapFieldRecord);

    GenericRecord rmdRecord = new GenericData.Record(timestampSchema);
    rmdRecord.put(TIMESTAMP_FIELD_NAME, timestampRecord);

    Assert.assertEquals(RmdUtils.getLastUpdateTimestamp(rmdRecord), 300L);
  }

  @Test
  public void testGetLastUpdateTimestampWithEmptyCollections() {
    RmdSchemaGeneratorV1 rmdSchemaGeneratorV1 = new RmdSchemaGeneratorV1();
    Schema timestampSchema = rmdSchemaGeneratorV1.generateMetadataSchema(TestWriteUtils.USER_WITH_STRING_MAP_SCHEMA);
    Schema mapFieldSchema = timestampSchema.getField("timestamp").schema().getTypes().get(1).getField("value").schema();

    GenericRecord mapFieldRecord = new GenericData.Record(mapFieldSchema);
    mapFieldRecord.put(ACTIVE_ELEM_TS_FIELD_NAME, Collections.emptyList());
    mapFieldRecord.put(TOP_LEVEL_TS_FIELD_NAME, 25L);
    mapFieldRecord.put(DELETED_ELEM_TS_FIELD_NAME, Collections.emptyList());

    GenericRecord timestampRecord =
        new GenericData.Record(timestampSchema.getField("timestamp").schema().getTypes().get(1));
    timestampRecord.put("key", 100L); // This should be the max
    timestampRecord.put("age", 50L);
    timestampRecord.put("value", mapFieldRecord);

    GenericRecord rmdRecord = new GenericData.Record(timestampSchema);
    rmdRecord.put(TIMESTAMP_FIELD_NAME, timestampRecord);

    // Should return 100L (max from key field since collections are empty)
    Assert.assertEquals(RmdUtils.getLastUpdateTimestamp(rmdRecord), 100L);
  }
}
