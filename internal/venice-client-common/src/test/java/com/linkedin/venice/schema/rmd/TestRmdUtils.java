package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import com.linkedin.venice.utils.TestWriteUtils;
import java.util.Arrays;
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
  public void testExtractLatestTimestampFromRmd() {
    Assert.assertEquals(30L, RmdUtils.getLastUpdateTimestamp(rmdRecordWithValidPerFieldLevelTimestamp));
    Assert.assertEquals(0L, RmdUtils.getLastUpdateTimestamp(rmdRecordWithOnlyRootLevelTimestamp));
  }
}
