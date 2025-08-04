package com.linkedin.venice.utils;

import com.linkedin.venice.protocols.CountByValueResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CountByValueUtilsTest {
  private static final String TEST_STORE_NAME = "test_store";
  private static final String JOB_TYPE_FIELD = "jobType";
  private static final String LOCATION_FIELD = "location";
  private static final String EXPERIENCE_FIELD = "experienceLevel";
  private static final String VALUE_FIELD = "value";
  private static final String UNDERSCORE_VALUE_FIELD = "_value";

  @Test
  public void testExtractFieldValueFromStringValuedStore() {
    String storeValue = "full-time";
    String result = CountByValueUtils.extractFieldValue(storeValue, VALUE_FIELD);
    Assert.assertEquals(result, "full-time");

    result = CountByValueUtils.extractFieldValue(storeValue, UNDERSCORE_VALUE_FIELD);
    Assert.assertEquals(result, "full-time");

    result = CountByValueUtils.extractFieldValue(storeValue, "invalid_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueFromUtf8ValuedStore() {
    Utf8 storeValue = new Utf8("remote");
    String result = CountByValueUtils.extractFieldValue(storeValue, VALUE_FIELD);
    Assert.assertEquals(result, "remote");

    result = CountByValueUtils.extractFieldValue(storeValue, UNDERSCORE_VALUE_FIELD);
    Assert.assertEquals(result, "remote");

    result = CountByValueUtils.extractFieldValue(storeValue, "non_existent_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueFromRecordValuedStore() {
    Schema jobRecordSchema = Schema.createRecord("JobRecord", null, null, false);
    jobRecordSchema.setFields(
        Arrays.asList(
            new Schema.Field(JOB_TYPE_FIELD, Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field(LOCATION_FIELD, Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("salary", Schema.create(Schema.Type.INT), null, null)));

    GenericRecord jobRecord = new GenericData.Record(jobRecordSchema);
    jobRecord.put(JOB_TYPE_FIELD, new Utf8("part-time"));
    jobRecord.put(LOCATION_FIELD, new Utf8("hybrid"));
    jobRecord.put("salary", 75000);

    String result = CountByValueUtils.extractFieldValue(jobRecord, JOB_TYPE_FIELD);
    Assert.assertEquals(result, "part-time");

    result = CountByValueUtils.extractFieldValue(jobRecord, LOCATION_FIELD);
    Assert.assertEquals(result, "hybrid");

    result = CountByValueUtils.extractFieldValue(jobRecord, "salary");
    Assert.assertEquals(result, "75000");

    result = CountByValueUtils.extractFieldValue(jobRecord, "non_existent_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueFromNullValue() {
    String result = CountByValueUtils.extractFieldValue(null, JOB_TYPE_FIELD);
    Assert.assertNull(result);
  }

  @Test
  public void testCountFieldOccurrencesWithJobTypes() {
    List<Object> jobTypes = Arrays.asList("full-time", "part-time", "full-time", "contract", "part-time", "full-time");
    List<String> fieldNames = Arrays.asList(VALUE_FIELD);

    Map<String, Map<String, Integer>> result = CountByValueUtils.countFieldOccurrences(jobTypes, fieldNames);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.containsKey(VALUE_FIELD));

    Map<String, Integer> jobTypeCounts = result.get(VALUE_FIELD);
    Assert.assertEquals(jobTypeCounts.get("full-time"), Integer.valueOf(3));
    Assert.assertEquals(jobTypeCounts.get("part-time"), Integer.valueOf(2));
    Assert.assertEquals(jobTypeCounts.get("contract"), Integer.valueOf(1));
  }

  @Test
  public void testCountFieldOccurrencesWithStoreKeyValuePairs() {
    Map<String, Object> storeData = new HashMap<>();
    storeData.put("key_1", "onsite");
    storeData.put("key_2", "remote");
    storeData.put("key_3", "onsite");
    storeData.put("key_4", "hybrid");

    List<String> fieldNames = Arrays.asList(VALUE_FIELD);

    Map<String, Map<String, Integer>> result = CountByValueUtils.countFieldOccurrences(storeData, fieldNames);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.containsKey(VALUE_FIELD));

    Map<String, Integer> locationCounts = result.get(VALUE_FIELD);
    Assert.assertEquals(locationCounts.get("onsite"), Integer.valueOf(2));
    Assert.assertEquals(locationCounts.get("remote"), Integer.valueOf(1));
    Assert.assertEquals(locationCounts.get("hybrid"), Integer.valueOf(1));
  }

  @Test
  public void testAddValueToFieldCountsWithStoreValues() {
    Map<String, Map<String, Integer>> fieldCounts = new HashMap<>();
    fieldCounts.put(VALUE_FIELD, new HashMap<>());

    List<String> fieldNames = Arrays.asList(VALUE_FIELD);

    CountByValueUtils.addValueToFieldCounts(fieldCounts, "senior", fieldNames);
    CountByValueUtils.addValueToFieldCounts(fieldCounts, "junior", fieldNames);
    CountByValueUtils.addValueToFieldCounts(fieldCounts, "senior", fieldNames);

    Map<String, Integer> experienceCounts = fieldCounts.get(VALUE_FIELD);
    Assert.assertEquals(experienceCounts.get("senior"), Integer.valueOf(2));
    Assert.assertEquals(experienceCounts.get("junior"), Integer.valueOf(1));
  }

  @Test
  public void testAddValueToFieldCountsWithNullInputs() {
    Map<String, Map<String, Integer>> fieldCounts = new HashMap<>();
    fieldCounts.put(VALUE_FIELD, new HashMap<>());
    List<String> fieldNames = Arrays.asList(VALUE_FIELD);

    CountByValueUtils.addValueToFieldCounts(fieldCounts, null, fieldNames);
    Assert.assertTrue(fieldCounts.get(VALUE_FIELD).isEmpty());

    CountByValueUtils.addValueToFieldCounts(null, "senior", fieldNames);
    // Should not throw exception
  }

  @Test
  public void testFilterTopKStoreValues() {
    Map<String, Integer> locationCounts = new HashMap<>();
    locationCounts.put("onsite", 50);
    locationCounts.put("remote", 30);
    locationCounts.put("hybrid", 80);
    locationCounts.put("field", 10);
    locationCounts.put("travel", 20);

    Map<String, Integer> result = CountByValueUtils.filterTopKValues(locationCounts, 3);
    Assert.assertEquals(result.size(), 3);
    Assert.assertTrue(result.containsKey("hybrid"));
    Assert.assertTrue(result.containsKey("onsite"));
    Assert.assertTrue(result.containsKey("remote"));
    Assert.assertEquals(result.get("hybrid"), Integer.valueOf(80));
    Assert.assertEquals(result.get("onsite"), Integer.valueOf(50));
    Assert.assertEquals(result.get("remote"), Integer.valueOf(30));

    result = CountByValueUtils.filterTopKValues(locationCounts, 10);
    Assert.assertEquals(result.size(), 5);
    Assert.assertEquals(result, locationCounts);

    result = CountByValueUtils.filterTopKValues(locationCounts, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey("hybrid"));
    Assert.assertEquals(result.get("hybrid"), Integer.valueOf(80));
  }

  @Test
  public void testFilterTopKValuesWithTiedCounts() {
    Map<String, Integer> experienceCounts = new HashMap<>();
    experienceCounts.put("mid", 3);
    experienceCounts.put("junior", 3);
    experienceCounts.put("senior", 5);

    Map<String, Integer> result = CountByValueUtils.filterTopKValues(experienceCounts, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("senior"));
    // Between mid and junior (both count 3), junior comes first alphabetically
    Assert.assertTrue(result.containsKey("junior"));
  }

  @Test
  public void testCountFieldOccurrencesWithMultipleStoreFields() {
    Schema jobRecordSchema = Schema.createRecord("JobRecord", null, null, false);
    jobRecordSchema.setFields(
        Arrays.asList(
            new Schema.Field(JOB_TYPE_FIELD, Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field(LOCATION_FIELD, Schema.create(Schema.Type.STRING), null, null)));

    GenericRecord job1 = new GenericData.Record(jobRecordSchema);
    job1.put(JOB_TYPE_FIELD, new Utf8("full-time"));
    job1.put(LOCATION_FIELD, new Utf8("onsite"));

    GenericRecord job2 = new GenericData.Record(jobRecordSchema);
    job2.put(JOB_TYPE_FIELD, new Utf8("full-time"));
    job2.put(LOCATION_FIELD, new Utf8("remote"));

    GenericRecord job3 = new GenericData.Record(jobRecordSchema);
    job3.put(JOB_TYPE_FIELD, new Utf8("part-time"));
    job3.put(LOCATION_FIELD, new Utf8("hybrid"));

    List<Object> jobs = Arrays.asList(job1, job2, job3);
    List<String> fieldNames = Arrays.asList(JOB_TYPE_FIELD, LOCATION_FIELD);

    Map<String, Map<String, Integer>> result = CountByValueUtils.countFieldOccurrences(jobs, fieldNames);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.containsKey(JOB_TYPE_FIELD));
    Assert.assertTrue(result.containsKey(LOCATION_FIELD));

    Map<String, Integer> jobTypeCounts = result.get(JOB_TYPE_FIELD);
    Assert.assertEquals(jobTypeCounts.get("full-time"), Integer.valueOf(2));
    Assert.assertEquals(jobTypeCounts.get("part-time"), Integer.valueOf(1));

    Map<String, Integer> locationCounts = result.get(LOCATION_FIELD);
    Assert.assertEquals(locationCounts.get("onsite"), Integer.valueOf(1));
    Assert.assertEquals(locationCounts.get("remote"), Integer.valueOf(1));
    Assert.assertEquals(locationCounts.get("hybrid"), Integer.valueOf(1));
  }

  @Test
  public void testMergePartitionResponses() {
    List<String> fieldNames = Arrays.asList(JOB_TYPE_FIELD, LOCATION_FIELD);

    // Create first partition response
    Map<String, Integer> partition1JobTypeCounts = new HashMap<>();
    partition1JobTypeCounts.put("full-time", 2);
    partition1JobTypeCounts.put("part-time", 1);

    Map<String, Integer> partition1LocationCounts = new HashMap<>();
    partition1LocationCounts.put("remote", 2);
    partition1LocationCounts.put("onsite", 1);

    CountByValueResponse response1 = CountByValueResponse.newBuilder()
        .putFieldToValueCounts(
            JOB_TYPE_FIELD,
            com.linkedin.venice.protocols.ValueCount.newBuilder().putAllValueToCounts(partition1JobTypeCounts).build())
        .putFieldToValueCounts(
            LOCATION_FIELD,
            com.linkedin.venice.protocols.ValueCount.newBuilder().putAllValueToCounts(partition1LocationCounts).build())
        .build();

    // Create second partition response
    Map<String, Integer> partition2JobTypeCounts = new HashMap<>();
    partition2JobTypeCounts.put("full-time", 1);
    partition2JobTypeCounts.put("contract", 2);

    Map<String, Integer> partition2LocationCounts = new HashMap<>();
    partition2LocationCounts.put("remote", 1);
    partition2LocationCounts.put("hybrid", 2);

    CountByValueResponse response2 = CountByValueResponse.newBuilder()
        .putFieldToValueCounts(
            JOB_TYPE_FIELD,
            com.linkedin.venice.protocols.ValueCount.newBuilder().putAllValueToCounts(partition2JobTypeCounts).build())
        .putFieldToValueCounts(
            LOCATION_FIELD,
            com.linkedin.venice.protocols.ValueCount.newBuilder().putAllValueToCounts(partition2LocationCounts).build())
        .build();

    // Merge responses
    List<CountByValueResponse> responses = Arrays.asList(response1, response2);
    Map<String, Map<String, Integer>> merged = CountByValueUtils.mergePartitionResponses(responses, fieldNames);

    // Verify merged results
    Assert.assertNotNull(merged);
    Assert.assertTrue(merged.containsKey(JOB_TYPE_FIELD));
    Assert.assertTrue(merged.containsKey(LOCATION_FIELD));

    // Verify job type counts (should be summed across partitions)
    Map<String, Integer> mergedJobTypeCounts = merged.get(JOB_TYPE_FIELD);
    Assert.assertEquals(mergedJobTypeCounts.get("full-time"), Integer.valueOf(3)); // 2 + 1
    Assert.assertEquals(mergedJobTypeCounts.get("part-time"), Integer.valueOf(1)); // 1 + 0
    Assert.assertEquals(mergedJobTypeCounts.get("contract"), Integer.valueOf(2)); // 0 + 2

    // Verify location counts (should be summed across partitions)
    Map<String, Integer> mergedLocationCounts = merged.get(LOCATION_FIELD);
    Assert.assertEquals(mergedLocationCounts.get("remote"), Integer.valueOf(3)); // 2 + 1
    Assert.assertEquals(mergedLocationCounts.get("onsite"), Integer.valueOf(1)); // 1 + 0
    Assert.assertEquals(mergedLocationCounts.get("hybrid"), Integer.valueOf(2)); // 0 + 2
  }
}
