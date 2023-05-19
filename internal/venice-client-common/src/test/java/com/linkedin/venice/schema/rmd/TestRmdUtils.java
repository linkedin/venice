package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import java.nio.ByteBuffer;
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
    rmdRecordWithValueLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, vectors);

    rmdRecordWithPerFieldLevelTimeStamp = new GenericData.Record(rmdSchema);
    // This is not a valid value for PER_FIELD_TIMESTAMP type. Use this for testing purpose only.
    rmdRecordWithPerFieldLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, new GenericData.Record(valueSchema));
    rmdRecordWithPerFieldLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, vectors);
  }

  @Test
  public void testDeserializeRmdBytes() {
    ByteBuffer bytes = RmdUtils.serializeRmdRecord(rmdSchema, rmdRecordWithValueLevelTimeStamp);
    GenericRecord reverted = RmdUtils.deserializeRmdBytes(rmdSchema, rmdSchema, bytes);
    Assert.assertEquals(reverted.getSchema().toString(), rmdRecordWithValueLevelTimeStamp.getSchema().toString());
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

    Assert.assertFalse(RmdUtils.hasOffsetAdvanced(list1, list2));
    Assert.assertFalse(RmdUtils.hasOffsetAdvanced(list1, Collections.emptyList()));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(list2, list1));
    Assert.assertTrue(RmdUtils.hasOffsetAdvanced(Collections.emptyList(), list2));
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
}
