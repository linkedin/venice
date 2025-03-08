package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
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

  private static final String RMD_COLLECTION_RECORD = "        {\n" + "            \"name\": \"creatives\",\n"
      + "            \"type\": {\n" + "                \"type\": \"record\",\n"
      + "                \"name\": \"map_CollectionMetadata_0\",\n"
      + "                \"doc\": \"structure that maintains all of the necessary metadata to perform deterministic conflict resolution on collection fields.\",\n"
      + "                \"fields\": [\n" + "                    {\n"
      + "                        \"name\": \"topLevelFieldTimestamp\",\n"
      + "                        \"type\": \"long\",\n"
      + "                        \"doc\": \"Timestamp of the last partial update attempting to set every element of this collection.\",\n"
      + "                        \"default\": 0\n" + "                    },\n" + "                    {\n"
      + "                        \"name\": \"topLevelColoID\",\n" + "                        \"type\": \"int\",\n"
      + "                        \"doc\": \"ID of the colo from which the last successfully applied partial update was sent.\",\n"
      + "                        \"default\": -1\n" + "                    },\n" + "                    {\n"
      + "                        \"name\": \"putOnlyPartLength\",\n" + "                        \"type\": \"int\",\n"
      + "                        \"doc\": \"Length of the put-only part of the collection which starts from index 0.\",\n"
      + "                        \"default\": 0\n" + "                    },\n" + "                    {\n"
      + "                        \"name\": \"activeElementsTimestamps\",\n" + "                        \"type\": {\n"
      + "                            \"type\": \"array\",\n" + "                            \"items\": \"long\"\n"
      + "                        },\n"
      + "                        \"doc\": \"Timestamps of each active element in the user's collection. This is a parallel array with the user's collection.\",\n"
      + "                        \"default\": []\n" + "                    },\n" + "                    {\n"
      + "                        \"name\": \"deletedElementsIdentities\",\n" + "                        \"type\": {\n"
      + "                            \"type\": \"array\",\n" + "                            \"items\": \"string\"\n"
      + "                        },\n"
      + "                        \"doc\": \"The tombstone array of deleted elements. This is a parallel array with deletedElementsTimestamps\",\n"
      + "                        \"default\": []\n" + "                    },\n" + "                    {\n"
      + "                        \"name\": \"deletedElementsTimestamps\",\n" + "                        \"type\": {\n"
      + "                            \"type\": \"array\",\n" + "                            \"items\": \"long\"\n"
      + "                        },\n"
      + "                        \"doc\": \"Timestamps of each deleted element. This is a parallel array with deletedElementsIdentity.\",\n"
      + "                        \"default\": []\n" + "                    }\n" + "                ]\n"
      + "            },\n" + "            \"doc\": \"timestamp when creatives of the record was last updated\",\n"
      + "            \"default\": {\n" + "                \"deletedElementsTimestamps\": [],\n"
      + "                \"deletedElementsIdentities\": [],\n" + "                \"topLevelColoID\": -1,\n"
      + "                \"putOnlyPartLength\": 0,\n" + "                \"activeElementsTimestamps\": [],\n"
      + "                \"topLevelFieldTimestamp\": 0\n" + "            }\n" + "        }";

  private static final String RMD_TIMESTAMP_RECORD =
      "{\n" + "    \"type\": \"record\",\n" + "    \"name\": \"ValueTest\",\n" + "    \"fields\": [\n" + "        {\n"
          + "            \"name\": \"account\",\n" + "            \"type\": \"long\",\n"
          + "            \"doc\": \"timestamp when account of the record was last updated\",\n"
          + "            \"default\": 0\n" + "        },\n" + "        {\n"
          + "            \"name\": \"campaignGroup\",\n" + "            \"type\": \"long\",\n"
          + "            \"doc\": \"timestamp when campaignGroup of the record was last updated\",\n"
          + "            \"default\": 0\n" + "        },\n" + "        {\n" + "            \"name\": \"campaign\",\n"
          + "            \"type\": \"long\",\n"
          + "            \"doc\": \"timestamp when campaign of the record was last updated\",\n"
          + "            \"default\": 0\n" + "        },\n" + RMD_COLLECTION_RECORD + "    ]\n" + "}";

  private static final String RMD_RECORD_SCHEMA_STR = "{\n" + "    \"type\": \"record\",\n"
      + "    \"name\": \"ValueTest_MetadataRecord\",\n" + "    \"fields\": [\n" + "        {\n"
      + "            \"name\": \"timestamp\",\n" + "            \"type\": [\n" + "                \"long\",\n"
      + RMD_TIMESTAMP_RECORD + "                \n" + "            ]\n" + "        },\n" + "        {\n"
      + "            \"name\": \"replication_checkpoint_vector\",\n" + "            \"type\": {\n"
      + "                \"type\": \"array\",\n" + "                \"items\": \"long\"\n" + "            },\n"
      + "            \"doc\": \"high watermark remote checkpoints which touched this record\",\n"
      + "            \"default\": []\n" + "        }\n" + "    ]\n" + "}";

  private Schema valueSchema;
  private Schema rmdSchema;
  private Schema rmdSchemaWithPerFieldTimestamp;
  private Schema timestampRecordSchema;
  private GenericRecord rmdRecordWithValueLevelTimeStamp;
  private GenericRecord rmdRecordWithPerFieldLevelTimeStamp;
  private GenericRecord rmdRecordWithValidPerFieldLevelTimestamp;
  private GenericRecord rmdRecordWithOnlyRootLevelTimestamp;
  private GenericRecord timestampRecord;

  @BeforeClass
  public void setUp() {
    valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_RECORD_SCHEMA_STR);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);
    rmdSchemaWithPerFieldTimestamp = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(RMD_RECORD_SCHEMA_STR);
    timestampRecordSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(RMD_TIMESTAMP_RECORD);
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
    rmdRecordWithValidPerFieldLevelTimestamp = new GenericData.Record(rmdSchemaWithPerFieldTimestamp);
    rmdRecordWithOnlyRootLevelTimestamp = new GenericData.Record(rmdSchemaWithPerFieldTimestamp);
    timestampRecord = new GenericData.Record(timestampRecordSchema);
    timestampRecord.put("account", 10L);
    timestampRecord.put("campaignGroup", 20L);
    timestampRecord.put("campaign", 30L);
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
    Assert.assertEquals(30L, RmdUtils.getLastUpdateTimestamp(rmdRecordWithValidPerFieldLevelTimestamp));
    Assert.assertEquals(0L, RmdUtils.getLastUpdateTimestamp(rmdRecordWithOnlyRootLevelTimestamp));
  }
}
