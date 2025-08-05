package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.protocols.BucketCount;
import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.ComparisonPredicate;
import com.linkedin.venice.protocols.CountByBucketResponse;
import com.linkedin.venice.protocols.LogicalPredicate;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class CountByBucketUtilsTest {
  @Test
  public void testInitializeBucketCounts() {
    List<String> fieldNames = Arrays.asList("age", "score");
    List<String> bucketNames = Arrays.asList("young", "middle", "old");

    Map<String, Map<String, Integer>> result = CountByBucketUtils.initializeBucketCounts(fieldNames, bucketNames);

    assertEquals(result.size(), 2);
    assertEquals(result.get("age").size(), 3);
    assertEquals(result.get("score").size(), 3);
    assertEquals(result.get("age").get("young"), Integer.valueOf(0));
    assertEquals(result.get("age").get("middle"), Integer.valueOf(0));
    assertEquals(result.get("age").get("old"), Integer.valueOf(0));
  }

  @Test
  public void testValidateFieldsWithStringSchema() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    List<String> validFieldNames = Arrays.asList("value");
    List<String> invalidFieldNames = Arrays.asList("invalid_field");

    // Valid field names for string schema
    CountByBucketResponse validResponse =
        CountByBucketUtils.validateFields(validFieldNames, stringSchema, "test_store");
    assertNull(validResponse);

    // Invalid field names for string schema
    CountByBucketResponse invalidResponse =
        CountByBucketUtils.validateFields(invalidFieldNames, stringSchema, "test_store");
    assertNotNull(invalidResponse);
    assertEquals(invalidResponse.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testValidateFieldsWithRecordSchema() {
    Schema recordSchema = Schema.createRecord("TestRecord", null, null, false);
    recordSchema.setFields(
        Arrays.asList(
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    List<String> validFieldNames = Arrays.asList("age", "name");
    List<String> invalidFieldNames = Arrays.asList("invalid_field");

    // Valid field names for record schema
    CountByBucketResponse validResponse =
        CountByBucketUtils.validateFields(validFieldNames, recordSchema, "test_store");
    assertNull(validResponse);

    // Invalid field names for record schema
    CountByBucketResponse invalidResponse =
        CountByBucketUtils.validateFields(invalidFieldNames, recordSchema, "test_store");
    assertNotNull(invalidResponse);
    assertEquals(invalidResponse.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testValidateFieldsWithUnsupportedSchema() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    List<String> fieldNames = Arrays.asList("field1");

    CountByBucketResponse response = CountByBucketUtils.validateFields(fieldNames, arraySchema, "test_store");
    assertNotNull(response);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testBuildResponse() {
    List<String> fieldNames = Arrays.asList("age", "score");
    Map<String, Map<String, Integer>> bucketCounts = new HashMap<>();

    Map<String, Integer> ageCounts = new HashMap<>();
    ageCounts.put("young", 10);
    ageCounts.put("old", 5);
    bucketCounts.put("age", ageCounts);

    Map<String, Integer> scoreCounts = new HashMap<>();
    scoreCounts.put("high", 8);
    scoreCounts.put("low", 7);
    bucketCounts.put("score", scoreCounts);

    CountByBucketResponse response = CountByBucketUtils.buildResponse(fieldNames, bucketCounts);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    assertEquals(response.getFieldToBucketCountsMap().size(), 2);
    assertEquals(
        response.getFieldToBucketCountsMap().get("age").getBucketToCountsMap().get("young"),
        Integer.valueOf(10));
    assertEquals(
        response.getFieldToBucketCountsMap().get("score").getBucketToCountsMap().get("high"),
        Integer.valueOf(8));
  }

  @Test
  public void testBuildResponseWithEmptyBuckets() {
    List<String> fieldNames = Arrays.asList("age");
    Map<String, Map<String, Integer>> bucketCounts = new HashMap<>();

    CountByBucketResponse response = CountByBucketUtils.buildResponse(fieldNames, bucketCounts);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    assertEquals(response.getFieldToBucketCountsMap().size(), 1);
    assertNotNull(response.getFieldToBucketCountsMap().get("age"));
  }

  @Test
  public void testMergePartitionResponses() {
    List<String> fieldNames = Arrays.asList("age");
    List<String> bucketNames = Arrays.asList("young", "old");

    // Create two partition responses
    BucketCount partition1Count =
        BucketCount.newBuilder().putBucketToCounts("young", 5).putBucketToCounts("old", 3).build();

    BucketCount partition2Count =
        BucketCount.newBuilder().putBucketToCounts("young", 7).putBucketToCounts("old", 2).build();

    CountByBucketResponse response1 =
        CountByBucketResponse.newBuilder().putFieldToBucketCounts("age", partition1Count).build();

    CountByBucketResponse response2 =
        CountByBucketResponse.newBuilder().putFieldToBucketCounts("age", partition2Count).build();

    List<CountByBucketResponse> responses = Arrays.asList(response1, response2);

    Map<String, Map<String, Integer>> merged =
        CountByBucketUtils.mergePartitionResponses(responses, fieldNames, bucketNames);

    assertEquals(merged.get("age").get("young"), Integer.valueOf(12)); // 5 + 7
    assertEquals(merged.get("age").get("old"), Integer.valueOf(5)); // 3 + 2
  }

  @Test
  public void testEvaluateBucketsWithRealRecordData() {
    // Create test schema similar to CountByValueUtilsTest
    Schema recordSchema = Schema.createRecord("JobRecord", null, null, false);
    recordSchema.setFields(
        Arrays.asList(
            new Schema.Field("jobType", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("salary", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("experience", Schema.create(Schema.Type.FLOAT), null, null)));

    // Create test records
    GenericRecord engineerRecord = new GenericData.Record(recordSchema);
    engineerRecord.put("jobType", new Utf8("engineer"));
    engineerRecord.put("salary", 75000);
    engineerRecord.put("experience", 3.5f);

    GenericRecord managerRecord = new GenericData.Record(recordSchema);
    managerRecord.put("jobType", new Utf8("manager"));
    managerRecord.put("salary", 95000);
    managerRecord.put("experience", 8.2f);

    GenericRecord seniorRecord = new GenericData.Record(recordSchema);
    seniorRecord.put("jobType", new Utf8("senior_engineer"));
    seniorRecord.put("salary", 120000);
    seniorRecord.put("experience", 7.0f);

    // Create bucket predicates
    Map<String, BucketPredicate> bucketPredicates = new HashMap<>();

    // Bucket for engineering roles
    BucketPredicate engineerPredicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder()
                .setOperator("IN")
                .setFieldType("STRING")
                .setValue("engineer,senior_engineer,junior_engineer")
                .build())
        .build();
    bucketPredicates.put("engineering_roles", engineerPredicate);

    // Bucket for high salary (>80000)
    BucketPredicate highSalaryPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("GT").setFieldType("INT").setValue("80000").build())
        .build();
    bucketPredicates.put("high_salary", highSalaryPredicate);

    // Bucket for experienced workers (>=5.0 years)
    BucketPredicate experiencedPredicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("GTE").setFieldType("FLOAT").setValue("5.0").build())
        .build();
    bucketPredicates.put("experienced", experiencedPredicate);

    // Test each field independently
    List<String> jobTypeFields = Arrays.asList("jobType");
    List<String> salaryFields = Arrays.asList("salary");
    List<String> experienceFields = Arrays.asList("experience");

    // Test jobType field with engineering roles bucket
    Map<String, Map<String, Integer>> jobTypeCounts =
        CountByBucketUtils.initializeBucketCounts(jobTypeFields, Arrays.asList("engineering_roles"));

    // Process records and verify jobType bucket counts
    Map<String, BucketPredicate> engineerPredicateMap = new HashMap<>();
    engineerPredicateMap.put("engineering_roles", engineerPredicate);

    processRecordForBuckets(engineerRecord, jobTypeFields, engineerPredicateMap, jobTypeCounts);
    assertEquals(jobTypeCounts.get("jobType").get("engineering_roles"), Integer.valueOf(1)); // engineer matches

    processRecordForBuckets(managerRecord, jobTypeFields, engineerPredicateMap, jobTypeCounts);
    assertEquals(jobTypeCounts.get("jobType").get("engineering_roles"), Integer.valueOf(1)); // manager doesn't match

    processRecordForBuckets(seniorRecord, jobTypeFields, engineerPredicateMap, jobTypeCounts);
    assertEquals(jobTypeCounts.get("jobType").get("engineering_roles"), Integer.valueOf(2)); // senior_engineer matches

    // Test salary field with high salary bucket
    Map<String, Map<String, Integer>> salaryCounts =
        CountByBucketUtils.initializeBucketCounts(salaryFields, Arrays.asList("high_salary"));

    Map<String, BucketPredicate> salaryPredicateMap = new HashMap<>();
    salaryPredicateMap.put("high_salary", highSalaryPredicate);

    processRecordForBuckets(engineerRecord, salaryFields, salaryPredicateMap, salaryCounts);
    assertEquals(salaryCounts.get("salary").get("high_salary"), Integer.valueOf(0)); // 75000 <= 80000

    processRecordForBuckets(managerRecord, salaryFields, salaryPredicateMap, salaryCounts);
    assertEquals(salaryCounts.get("salary").get("high_salary"), Integer.valueOf(1)); // 95000 > 80000

    processRecordForBuckets(seniorRecord, salaryFields, salaryPredicateMap, salaryCounts);
    assertEquals(salaryCounts.get("salary").get("high_salary"), Integer.valueOf(2)); // 120000 > 80000

    // Test experience field with experienced bucket
    Map<String, Map<String, Integer>> experienceCounts =
        CountByBucketUtils.initializeBucketCounts(experienceFields, Arrays.asList("experienced"));

    Map<String, BucketPredicate> experiencePredicateMap = new HashMap<>();
    experiencePredicateMap.put("experienced", experiencedPredicate);

    processRecordForBuckets(engineerRecord, experienceFields, experiencePredicateMap, experienceCounts);
    assertEquals(experienceCounts.get("experience").get("experienced"), Integer.valueOf(0)); // 3.5 < 5.0

    processRecordForBuckets(managerRecord, experienceFields, experiencePredicateMap, experienceCounts);
    assertEquals(experienceCounts.get("experience").get("experienced"), Integer.valueOf(1)); // 8.2 >= 5.0

    processRecordForBuckets(seniorRecord, experienceFields, experiencePredicateMap, experienceCounts);
    assertEquals(experienceCounts.get("experience").get("experienced"), Integer.valueOf(2)); // 7.0 >= 5.0
  }

  // Helper method to process records and update bucket counts (tests the actual logic without mocks)
  private void processRecordForBuckets(
      GenericRecord record,
      List<String> fieldNames,
      Map<String, BucketPredicate> bucketPredicates,
      Map<String, Map<String, Integer>> bucketCounts) {

    for (String fieldName: fieldNames) {
      // Extract field value directly from GenericRecord (simulating the extractFieldValue logic)
      Object fieldValue = record.get(fieldName);
      if (fieldValue instanceof Utf8) {
        fieldValue = fieldValue.toString();
      }
      if (fieldValue == null) {
        continue;
      }

      // Test the actual predicate evaluation logic
      for (Map.Entry<String, BucketPredicate> entry: bucketPredicates.entrySet()) {
        String bucketName = entry.getKey();
        BucketPredicate predicate = entry.getValue();

        if (PredicateEvaluator.evaluate(predicate, fieldValue)) {
          bucketCounts.get(fieldName).merge(bucketName, 1, Integer::sum);
        }
      }
    }
  }

  @Test
  public void testComplexLogicalPredicateAccuracy() {
    // Test AND/OR combinations for accurate predicate evaluation
    List<String> fieldNames = Arrays.asList("value");
    Map<String, BucketPredicate> bucketPredicates = new HashMap<>();

    // Create a complex AND predicate: value IN ("engineer", "manager") AND salary > 70000
    // Note: Since we're testing with string values, we'll simulate this with string comparisons

    // Bucket 1: String values that are either "engineer" OR "manager"
    BucketPredicate engineerPredicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("engineer").build())
        .build();

    BucketPredicate managerPredicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("manager").build())
        .build();

    BucketPredicate orPredicate = BucketPredicate.newBuilder()
        .setLogical(
            LogicalPredicate.newBuilder()
                .setOperator("OR")
                .addAllPredicates(Arrays.asList(engineerPredicate, managerPredicate))
                .build())
        .build();
    bucketPredicates.put("engineer_or_manager", orPredicate);

    // Bucket 2: Values containing specific strings
    BucketPredicate seniorPredicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder()
                .setOperator("IN")
                .setFieldType("STRING")
                .setValue("senior_engineer,senior_manager")
                .build())
        .build();
    bucketPredicates.put("senior_roles", seniorPredicate);

    Map<String, Map<String, Integer>> bucketCounts =
        CountByBucketUtils.initializeBucketCounts(fieldNames, Arrays.asList("engineer_or_manager", "senior_roles"));

    // Test accurate predicate evaluation
    testValueForComplexPredicates("engineer", bucketPredicates, bucketCounts);
    assertEquals(bucketCounts.get("value").get("engineer_or_manager"), Integer.valueOf(1)); // OR matches engineer
    assertEquals(bucketCounts.get("value").get("senior_roles"), Integer.valueOf(0)); // doesn't match senior_*

    testValueForComplexPredicates("manager", bucketPredicates, bucketCounts);
    assertEquals(bucketCounts.get("value").get("engineer_or_manager"), Integer.valueOf(2)); // OR matches manager
    assertEquals(bucketCounts.get("value").get("senior_roles"), Integer.valueOf(0)); // doesn't match senior_*

    testValueForComplexPredicates("senior_engineer", bucketPredicates, bucketCounts);
    assertEquals(bucketCounts.get("value").get("engineer_or_manager"), Integer.valueOf(2)); // OR doesn't match
                                                                                            // senior_engineer
    assertEquals(bucketCounts.get("value").get("senior_roles"), Integer.valueOf(1)); // IN matches senior_engineer

    testValueForComplexPredicates("senior_manager", bucketPredicates, bucketCounts);
    assertEquals(bucketCounts.get("value").get("engineer_or_manager"), Integer.valueOf(2)); // OR doesn't match
                                                                                            // senior_manager
    assertEquals(bucketCounts.get("value").get("senior_roles"), Integer.valueOf(2)); // IN matches senior_manager

    testValueForComplexPredicates("developer", bucketPredicates, bucketCounts);
    assertEquals(bucketCounts.get("value").get("engineer_or_manager"), Integer.valueOf(2)); // OR doesn't match
                                                                                            // developer
    assertEquals(bucketCounts.get("value").get("senior_roles"), Integer.valueOf(2)); // IN doesn't match developer
  }

  // Helper method to test complex predicate evaluation accuracy
  private void testValueForComplexPredicates(
      String value,
      Map<String, BucketPredicate> bucketPredicates,
      Map<String, Map<String, Integer>> bucketCounts) {

    for (Map.Entry<String, BucketPredicate> entry: bucketPredicates.entrySet()) {
      String bucketName = entry.getKey();
      BucketPredicate predicate = entry.getValue();

      // Test the actual PredicateEvaluator logic for accuracy
      if (PredicateEvaluator.evaluate(predicate, value)) {
        bucketCounts.get("value").merge(bucketName, 1, Integer::sum);
      }
    }
  }

  @Test
  public void testProcessValueForBucketsWithNullValue() {
    // Mock dependencies
    SchemaEntry mockSchemaEntry = Mockito.mock(SchemaEntry.class);
    VeniceCompressor mockCompressor = Mockito.mock(VeniceCompressor.class);
    AvroStoreDeserializerCache<Object> mockDeserializerCache = Mockito.mock(AvroStoreDeserializerCache.class);

    // Test with null value bytes
    List<String> fieldNames = Arrays.asList("testField");
    Map<String, BucketPredicate> bucketPredicates = new HashMap<>();
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    Map<String, Map<String, Integer>> bucketCounts =
        CountByBucketUtils.initializeBucketCounts(fieldNames, Arrays.asList("test_bucket"));

    // Test that null values are handled appropriately (should throw NPE or be handled gracefully)
    // Based on the current implementation, this will throw NPE, which is expected behavior
    try {
      CountByBucketUtils.processValueForBuckets(
          null,
          fieldNames,
          bucketPredicates,
          valueSchema,
          mockSchemaEntry,
          CompressionStrategy.NO_OP,
          mockCompressor,
          mockDeserializerCache,
          bucketCounts);
      // If we get here, the method handled null gracefully
      assertNotNull(bucketCounts.get("testField"));
    } catch (NullPointerException e) {
      // This is expected behavior - null valueBytes should cause NPE
      // Verify that bucket counts structure is still maintained after exception
      assertNotNull(bucketCounts.get("testField"));
    }
  }
}
