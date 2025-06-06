package com.linkedin.venice.client.store.predicate;

import static com.linkedin.venice.client.store.predicate.Predicate.and;
import static com.linkedin.venice.client.store.predicate.Predicate.equalTo;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PredicateTest {
  private static final String STRING_FIELD_VALUE = "helloWorld";
  private static final int INT_FIELD_VALUE = 123456;
  private static final List<Float> FLOAT_ARRAY_FIELD_VALUE = Arrays.asList(0.0f, 1.1f, 2.2f, 3.3f, 4.4f);
  private static final Schema KEY_SCHEMA = new Schema.Parser().parse(
      "{" + "  \"namespace\": \"example.predicate\"," //
          + "  \"type\": \"record\"," //
          + "  \"name\": \"KeyRecord\"," //
          + "  \"fields\": [" //
          + "         { \"name\": \"stringField\", \"type\": \"string\" }," //
          + "         { \"name\": \"intField\", \"type\": \"int\" }," //
          + "         {   \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }" //
          + "  ]" //
          + " }");

  @Test
  public void testPredicatesAllMatchingFields() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesOneMatchingField() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest = equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesMisMatchedArrayElement() {
    // N.B.: The last item is different
    List<Float> expectedFloatArrayFieldValue = Arrays.asList(0.0f, 1.1f, 2.2f, 3.3f, 5.5f);

    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", expectedFloatArrayFieldValue));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesNullRecordToTest() {
    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE));

    Assert.assertFalse(predicateToTest.evaluate(null));
  }

  @Test
  public void testPredicatesNonExistingField() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest =
        and(equalTo("stringField", STRING_FIELD_VALUE), equalTo("nonExistentField", "fakeValue"));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesNullExpectedFields() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", null);
    keyRecord.put("intField", null);
    keyRecord.put("floatArrayField", null);

    Predicate<GenericRecord> predicateToTest =
        and(equalTo("stringField", null), equalTo("intField", null), equalTo("floatArrayField", null));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesUnexpectedNullField() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", null);
    keyRecord.put("intField", null);
    keyRecord.put("floatArrayField", null);

    Predicate<GenericRecord> predicateToTest = equalTo("stringField", STRING_FIELD_VALUE);

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesDeepEquals() {
    String stringFieldValueCopy = "helloWorld";
    int intFieldValueCopy = 123456;
    List<Float> floatArrayFieldValueCopy = new ArrayList(FLOAT_ARRAY_FIELD_VALUE);

    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", stringFieldValueCopy),
        equalTo("intField", intFieldValueCopy),
        equalTo("floatArrayField", floatArrayFieldValueCopy));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesNestedAnds() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate<GenericRecord> predicateToTest = and(
        and(equalTo("stringField", STRING_FIELD_VALUE)),
        and(),
        and(equalTo("intField", INT_FIELD_VALUE), equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE)));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesMatchingNestedFields() {

    String nestedRecordSchemaString = "{ " //
        + "                         \"type\" : \"record\", \n" //
        + "                         \"name\" : \"KeyRecord2\",\n" //
        + "                         \"fields\" : [\n" //
        + "                            {\"name\" : \"stringField\", \"type\" : \"string\"}, \n" //
        + "                            {\"name\" : \"booleanField\", \"type\" : \"boolean\"} ]}";

    Schema nestedRecordKeySchema = new Schema.Parser().parse(nestedRecordSchemaString);

    Schema fullRecordKeySchema = new Schema.Parser().parse(
        "{" + "  \"namespace\": \"example.predicate\"," //
            + "  \"type\": \"record\"," //
            + "  \"name\": \"KeyRecord\"," //
            + "  \"fields\": [" //
            + "         { \"name\": \"stringField\", \"type\": \"string\" }," //
            + "         { \"name\": \"intField\", \"type\": \"int\" }," //
            + "         { \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }," //
            + "         { \"name\": \"nestedRecord\", \"type\": " + nestedRecordSchemaString + "}  ]" //
            + " }       ");

    GenericRecord nestedRecord = new GenericData.Record(nestedRecordKeySchema);
    nestedRecord.put("stringField", STRING_FIELD_VALUE);
    nestedRecord.put("booleanField", true);

    GenericRecord keyRecord = new GenericData.Record(fullRecordKeySchema);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);
    keyRecord.put("nestedRecord", nestedRecord);

    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE),
        equalTo("nestedRecord", nestedRecord));

    Predicate<GenericRecord> nestedPredicateToTest =
        and(equalTo("stringField", STRING_FIELD_VALUE), equalTo("booleanField", true));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
    Assert.assertTrue(nestedPredicateToTest.evaluate((GenericRecord) keyRecord.get("nestedRecord")));
  }

  @Test
  public void testPredicatesMisMatchedNestedFields() {

    String nestedRecordSchemaString = "{" //
        + "                         \"type\" : \"record\", \n" //
        + "                         \"name\" : \"KeyRecord2\",\n" //
        + "                         \"fields\" : [\n" //
        + "                            {\"name\" : \"stringField\", \"type\" : \"string\"}, \n" //
        + "                            {\"name\" : \"booleanField\", \"type\" : \"boolean\"} ]}";

    Schema nestedRecordKeySchema = new Schema.Parser().parse(nestedRecordSchemaString);

    Schema fullRecordKeySchema = new Schema.Parser().parse(
        "{" //
            + "  \"namespace\": \"example.predicate\"," //
            + "  \"type\": \"record\"," //
            + "  \"name\": \"KeyRecord\"," //
            + "  \"fields\": [" //
            + "         { \"name\": \"stringField\", \"type\": \"string\" }," //
            + "         { \"name\": \"intField\", \"type\": \"int\" }," //
            + "         { \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }," //
            + "         { \"name\": \"nestedRecord\", \"type\": " + nestedRecordSchemaString + "}  ]" //
            + " }");

    GenericRecord actualNestedRecord = new GenericData.Record(nestedRecordKeySchema);
    actualNestedRecord.put("stringField", STRING_FIELD_VALUE);
    actualNestedRecord.put("booleanField", true);

    GenericRecord expectedNestedRecord = new GenericData.Record(nestedRecordKeySchema);
    expectedNestedRecord.put("stringField", "mismatched string");
    expectedNestedRecord.put("booleanField", false);

    GenericRecord keyRecord = new GenericData.Record(fullRecordKeySchema);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);
    keyRecord.put("nestedRecord", actualNestedRecord);

    Predicate<GenericRecord> predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE),
        equalTo("nestedRecord", expectedNestedRecord));

    Predicate<GenericRecord> nestedPredicateToTest =
        and(equalTo("stringField", "mismatched string"), equalTo("booleanField", false));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
    Assert.assertFalse(nestedPredicateToTest.evaluate((GenericRecord) keyRecord.get("nestedRecord")));
  }

  @Test
  public void testPredicateCreationWithNullFieldName() {
    Assert.assertThrows(NullPointerException.class, () -> and(equalTo(null, null)));
  }

  @Test
  public void testIntPredicates() {
    assertTrue(IntPredicate.equalTo(1).evaluate(1));
    assertFalse(IntPredicate.equalTo(2).evaluate(1));

    assertTrue(IntPredicate.greaterThan(1).evaluate(2));
    assertFalse(IntPredicate.greaterThan(1).evaluate(1));

    assertTrue(IntPredicate.greaterOrEquals(1).evaluate(1));
    assertFalse(IntPredicate.greaterOrEquals(2).evaluate(1));

    assertTrue(IntPredicate.lowerThan(2).evaluate(1));
    assertFalse(IntPredicate.lowerThan(1).evaluate(1));

    assertTrue(IntPredicate.lowerOrEquals(1).evaluate(1));
    assertFalse(IntPredicate.lowerOrEquals(1).evaluate(2));

    Predicate betweenPredicate = Predicate.and(IntPredicate.greaterThan(1), IntPredicate.lowerOrEquals(3));
    assertFalse(betweenPredicate.evaluate(1));
    assertTrue(betweenPredicate.evaluate(2));
    assertTrue(betweenPredicate.evaluate(3));
    assertFalse(betweenPredicate.evaluate(4));

    Predicate outsideOfRange = Predicate.or(IntPredicate.lowerThan(1), IntPredicate.greaterThan(3));
    assertTrue(outsideOfRange.evaluate(0));
    assertFalse(outsideOfRange.evaluate(1));
    assertFalse(outsideOfRange.evaluate(2));
    assertFalse(outsideOfRange.evaluate(3));
    assertTrue(outsideOfRange.evaluate(4));

    IntPredicate anyOf = IntPredicate.anyOf(3, 5, 10);
    assertFalse(anyOf.evaluate(1));
    assertFalse(anyOf.evaluate(2));
    assertTrue(anyOf.evaluate(3));
    assertFalse(anyOf.evaluate(4));
    assertTrue(anyOf.evaluate(5));
    assertFalse(anyOf.evaluate(6));
    assertFalse(anyOf.evaluate(7));
    assertFalse(anyOf.evaluate(8));
    assertFalse(anyOf.evaluate(9));
    assertTrue(anyOf.evaluate(10));
    assertFalse(anyOf.evaluate(11));
  }

  @Test
  public void testLongPredicates() {
    assertTrue(LongPredicate.equalTo(1L).evaluate(1L));
    assertFalse(LongPredicate.equalTo(2L).evaluate(1L));

    assertTrue(LongPredicate.greaterThan(1L).evaluate(2L));
    assertFalse(LongPredicate.greaterThan(1L).evaluate(1L));

    assertTrue(LongPredicate.greaterOrEquals(1L).evaluate(1L));
    assertFalse(LongPredicate.greaterOrEquals(2L).evaluate(1L));

    assertTrue(LongPredicate.lowerThan(2L).evaluate(1L));
    assertFalse(LongPredicate.lowerThan(1L).evaluate(1L));

    assertTrue(LongPredicate.lowerOrEquals(1L).evaluate(1L));
    assertFalse(LongPredicate.lowerOrEquals(1L).evaluate(2L));

    Predicate betweenPredicate = Predicate.and(LongPredicate.greaterThan(1L), LongPredicate.lowerOrEquals(3L));
    assertFalse(betweenPredicate.evaluate(1L));
    assertTrue(betweenPredicate.evaluate(2L));
    assertTrue(betweenPredicate.evaluate(3L));
    assertFalse(betweenPredicate.evaluate(4L));

    Predicate outsideOfRange = Predicate.or(LongPredicate.lowerThan(1L), LongPredicate.greaterThan(3L));
    assertTrue(outsideOfRange.evaluate(0L));
    assertFalse(outsideOfRange.evaluate(1L));
    assertFalse(outsideOfRange.evaluate(2L));
    assertFalse(outsideOfRange.evaluate(3L));
    assertTrue(outsideOfRange.evaluate(4L));

    LongPredicate anyOf = LongPredicate.anyOf(3L, 5L, 10L);
    assertFalse(anyOf.evaluate(1L));
    assertFalse(anyOf.evaluate(2L));
    assertTrue(anyOf.evaluate(3L));
    assertFalse(anyOf.evaluate(4L));
    assertTrue(anyOf.evaluate(5L));
    assertFalse(anyOf.evaluate(6L));
    assertFalse(anyOf.evaluate(7L));
    assertFalse(anyOf.evaluate(8L));
    assertFalse(anyOf.evaluate(9L));
    assertTrue(anyOf.evaluate(10L));
    assertFalse(anyOf.evaluate(11L));
  }

  @Test
  public void testTimeBucketGroupingPredicates() {
    Schema timeBucketSchema = new Schema.Parser().parse(
        "{\n" + "  \"namespace\": \"example.predicate\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"TimeBucketRecord\",\n" + "  \"fields\": [\n"
            + "    { \"name\": \"timestamp\", \"type\": \"long\" },\n"
            + "    { \"name\": \"value\", \"type\": \"string\" }\n" + "  ]\n" + "}");

    // Create test records with fixed timestamps for better predictability
    long baseTime = 1617235200000L; // 2021-04-01 00:00:00 UTC
    long hourInMillis = 3600000L; // 1 hour in milliseconds
    long dayInMillis = 24 * hourInMillis;

    // Create records with specific timestamps
    long lastHourTimestamp = baseTime - hourInMillis / 2; // 30 minutes before baseTime
    long twoHoursAgoTimestamp = baseTime - 2 * hourInMillis; // 2 hours before baseTime
    long yesterdayTimestamp = baseTime - 26 * hourInMillis; // 26 hours before baseTime

    GenericRecord recordLastHour = new GenericData.Record(timeBucketSchema);
    recordLastHour.put("timestamp", lastHourTimestamp);
    recordLastHour.put("value", "last hour");

    GenericRecord recordTwoHoursAgo = new GenericData.Record(timeBucketSchema);
    recordTwoHoursAgo.put("timestamp", twoHoursAgoTimestamp);
    recordTwoHoursAgo.put("value", "two hours ago");

    GenericRecord recordYesterday = new GenericData.Record(timeBucketSchema);
    recordYesterday.put("timestamp", yesterdayTimestamp);
    recordYesterday.put("value", "yesterday");

    // Test hourly bucket using LongPredicate
    long hourStart = baseTime - hourInMillis;
    long hourEnd = baseTime;
    Predicate<GenericRecord> lastHourBucket = and(
        equalTo("timestamp", LongPredicate.greaterOrEquals(hourStart)),
        equalTo("timestamp", LongPredicate.lowerThan(hourEnd)));

    assertTrue(lastHourBucket.evaluate(recordLastHour), "Record from last hour should be in the last hour bucket");
    assertFalse(
        lastHourBucket.evaluate(recordTwoHoursAgo),
        "Record from two hours ago should not be in the last hour bucket");
    assertFalse(
        lastHourBucket.evaluate(recordYesterday),
        "Record from yesterday should not be in the last hour bucket");

    // Test daily bucket using LongPredicate
    long dayStart = baseTime - dayInMillis;
    long dayEnd = baseTime;
    Predicate<GenericRecord> lastDayBucket = and(
        equalTo("timestamp", LongPredicate.greaterOrEquals(dayStart)),
        equalTo("timestamp", LongPredicate.lowerThan(dayEnd)));

    assertTrue(lastDayBucket.evaluate(recordLastHour), "Record from last hour should be in the last day bucket");
    assertTrue(lastDayBucket.evaluate(recordTwoHoursAgo), "Record from two hours ago should be in the last day bucket");
    assertFalse(lastDayBucket.evaluate(recordYesterday), "Record from yesterday should not be in the last day bucket");

    // Test multiple time ranges
    long yesterdayHourStart = baseTime - dayInMillis - hourInMillis;
    long yesterdayHourEnd = baseTime - dayInMillis;
    Predicate<GenericRecord> multiRangeBucket = or(
        // Last hour
        and(
            equalTo("timestamp", LongPredicate.greaterOrEquals(hourStart)),
            equalTo("timestamp", LongPredicate.lowerThan(hourEnd))),
        // Yesterday's same hour
        and(
            equalTo("timestamp", LongPredicate.greaterOrEquals(yesterdayHourStart)),
            equalTo("timestamp", LongPredicate.lowerThan(yesterdayHourEnd))));

    assertTrue(multiRangeBucket.evaluate(recordLastHour), "Record from last hour should be in multi-range bucket");
    assertFalse(
        multiRangeBucket.evaluate(recordTwoHoursAgo),
        "Record from two hours ago should not be in multi-range bucket");
    assertFalse(
        multiRangeBucket.evaluate(recordYesterday),
        "Record from yesterday should not be in multi-range bucket");

    // Test edge cases
    GenericRecord recordExactlyOneHourAgo = new GenericData.Record(timeBucketSchema);
    recordExactlyOneHourAgo.put("timestamp", hourStart);
    recordExactlyOneHourAgo.put("value", "exactly one hour ago");

    GenericRecord recordNow = new GenericData.Record(timeBucketSchema);
    recordNow.put("timestamp", hourEnd);
    recordNow.put("value", "now");

    assertTrue(
        lastHourBucket.evaluate(recordExactlyOneHourAgo),
        "Record from exactly one hour ago should be in the last hour bucket");
    assertFalse(lastHourBucket.evaluate(recordNow), "Record from now should not be in the last hour bucket");
  }

  @Test
  public void testCustomBucketSizeGrouping() {
    Schema bucketSchema = new Schema.Parser().parse(
        "{\n" + "  \"namespace\": \"example.predicate\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"CustomBucketRecord\",\n" + "  \"fields\": [\n"
            + "    { \"name\": \"value\", \"type\": \"long\" }\n" + "  ]\n" + "}");

    // Create records in different buckets
    GenericRecord recordInBucket1 = new GenericData.Record(bucketSchema);
    recordInBucket1.put("value", 150L); // Should be in bucket 100-200

    GenericRecord recordInBucket2 = new GenericData.Record(bucketSchema);
    recordInBucket2.put("value", 250L); // Should be in bucket 200-300

    // Create bucket predicates using LongPredicate
    Predicate<GenericRecord> bucket1 =
        and(equalTo("value", LongPredicate.greaterOrEquals(100L)), equalTo("value", LongPredicate.lowerThan(200L)));

    Predicate<GenericRecord> bucket2 =
        and(equalTo("value", LongPredicate.greaterOrEquals(200L)), equalTo("value", LongPredicate.lowerThan(300L)));

    // Test bucket membership
    assertTrue(bucket1.evaluate(recordInBucket1), "Record with value 150 should be in bucket 1");
    assertFalse(bucket1.evaluate(recordInBucket2), "Record with value 250 should not be in bucket 1");

    assertFalse(bucket2.evaluate(recordInBucket1), "Record with value 150 should not be in bucket 2");
    assertTrue(bucket2.evaluate(recordInBucket2), "Record with value 250 should be in bucket 2");

    // Test edge cases
    GenericRecord recordAtBucketStart = new GenericData.Record(bucketSchema);
    recordAtBucketStart.put("value", 200L); // At start of bucket 2

    GenericRecord recordAtBucketEnd = new GenericData.Record(bucketSchema);
    recordAtBucketEnd.put("value", 300L); // At end of bucket 2

    assertTrue(bucket2.evaluate(recordAtBucketStart), "Record with value at bucket start should be in the bucket");
    assertFalse(bucket2.evaluate(recordAtBucketEnd), "Record with value at bucket end should not be in the bucket");
  }

  @Test
  public void testIntBucketGroupingPredicates() {
    Schema bucketSchema = new Schema.Parser().parse(
        "{\n" + "  \"namespace\": \"example.predicate\",\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"IntBucketRecord\",\n" + "  \"fields\": [\n"
            + "    { \"name\": \"value\", \"type\": \"int\" }\n" + "  ]\n" + "}");

    // Create records in different buckets
    GenericRecord recordInBucket1 = new GenericData.Record(bucketSchema);
    recordInBucket1.put("value", 15); // Should be in bucket 10-20

    GenericRecord recordInBucket2 = new GenericData.Record(bucketSchema);
    recordInBucket2.put("value", 25); // Should be in bucket 20-30

    // Create bucket predicates using IntPredicate
    Predicate<GenericRecord> bucket1 =
        and(equalTo("value", IntPredicate.greaterOrEquals(10)), equalTo("value", IntPredicate.lowerThan(20)));

    Predicate<GenericRecord> bucket2 =
        and(equalTo("value", IntPredicate.greaterOrEquals(20)), equalTo("value", IntPredicate.lowerThan(30)));

    // Test bucket membership
    assertTrue(bucket1.evaluate(recordInBucket1), "Record with value 15 should be in bucket 1");
    assertFalse(bucket1.evaluate(recordInBucket2), "Record with value 25 should not be in bucket 1");

    assertFalse(bucket2.evaluate(recordInBucket1), "Record with value 15 should not be in bucket 2");
    assertTrue(bucket2.evaluate(recordInBucket2), "Record with value 25 should be in bucket 2");

    // Test edge cases
    GenericRecord recordAtBucketStart = new GenericData.Record(bucketSchema);
    recordAtBucketStart.put("value", 20); // At start of bucket 2

    GenericRecord recordAtBucketEnd = new GenericData.Record(bucketSchema);
    recordAtBucketEnd.put("value", 30); // At end of bucket 2

    assertTrue(bucket2.evaluate(recordAtBucketStart), "Record with value at bucket start should be in the bucket");
    assertFalse(bucket2.evaluate(recordAtBucketEnd), "Record with value at bucket end should not be in the bucket");

  }
}
