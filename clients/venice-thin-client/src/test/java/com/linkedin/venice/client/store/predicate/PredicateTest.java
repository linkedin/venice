package com.linkedin.venice.client.store.predicate;

import static com.linkedin.venice.client.store.predicate.Predicate.and;
import static com.linkedin.venice.client.store.predicate.Predicate.equalTo;
import static com.linkedin.venice.client.store.predicate.Predicate.or;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    long hourInMillis = TimeUnit.HOURS.toMillis(1);
    long dayInMillis = TimeUnit.DAYS.toMillis(1);

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

  @Test
  public void testFloatPredicates() {
    float epsilon = 1e-6f;

    // Basic equality with epsilon
    assertTrue(FloatPredicate.equalTo(1.0f, epsilon).evaluate(1.0f));
    assertTrue(FloatPredicate.equalTo(1.0f, epsilon).evaluate(1.0f + epsilon / 2));
    assertFalse(FloatPredicate.equalTo(1.0f, epsilon).evaluate(1.0f + epsilon * 2));

    // Basic comparisons
    assertTrue(FloatPredicate.greaterThan(1.0f).evaluate(2.0f));
    assertFalse(FloatPredicate.greaterThan(1.0f).evaluate(1.0f));

    assertTrue(FloatPredicate.greaterOrEquals(1.0f).evaluate(1.0f));
    assertFalse(FloatPredicate.greaterOrEquals(2.0f).evaluate(1.0f));

    assertTrue(FloatPredicate.lowerThan(2.0f).evaluate(1.0f));
    assertFalse(FloatPredicate.lowerThan(1.0f).evaluate(1.0f));

    assertTrue(FloatPredicate.lowerOrEquals(1.0f).evaluate(1.0f));
    assertFalse(FloatPredicate.lowerOrEquals(1.0f).evaluate(2.0f));

    // AnyOf predicate
    FloatPredicate anyOf = FloatPredicate.anyOf(3.0f, 5.0f, 10.0f);
    assertFalse(anyOf.evaluate(1.0f));
    assertFalse(anyOf.evaluate(2.0f));
    assertTrue(anyOf.evaluate(3.0f));
    assertFalse(anyOf.evaluate(4.0f));
    assertTrue(anyOf.evaluate(5.0f));
    assertFalse(anyOf.evaluate(6.0f));
    assertFalse(anyOf.evaluate(7.0f));
    assertFalse(anyOf.evaluate(8.0f));
    assertFalse(anyOf.evaluate(9.0f));
    assertTrue(anyOf.evaluate(10.0f));
    assertFalse(anyOf.evaluate(11.0f));

    // Special values tests
    assertTrue(FloatPredicate.equalTo(Float.NaN, epsilon).evaluate(Float.NaN));
    assertTrue(FloatPredicate.equalTo(Float.POSITIVE_INFINITY, epsilon).evaluate(Float.POSITIVE_INFINITY));
    assertTrue(FloatPredicate.equalTo(Float.NEGATIVE_INFINITY, epsilon).evaluate(Float.NEGATIVE_INFINITY));

    // Null handling
    assertFalse(FloatPredicate.equalTo(1.0f, epsilon).evaluate(null));
    assertFalse(FloatPredicate.greaterThan(1.0f).evaluate(null));
    assertFalse(FloatPredicate.anyOf(1.0f, 2.0f).evaluate(null));

    // Composition tests
    Predicate<Float> betweenPredicate =
        Predicate.and(FloatPredicate.greaterThan(1.0f), FloatPredicate.lowerOrEquals(3.0f));
    assertFalse(betweenPredicate.evaluate(1.0f));
    assertTrue(betweenPredicate.evaluate(2.0f));
    assertTrue(betweenPredicate.evaluate(3.0f));
    assertFalse(betweenPredicate.evaluate(4.0f));

    Predicate<Float> outsideRangePredicate =
        Predicate.or(FloatPredicate.lowerThan(1.0f), FloatPredicate.greaterThan(3.0f));
    assertTrue(outsideRangePredicate.evaluate(0.5f));
    assertFalse(outsideRangePredicate.evaluate(1.5f));
    assertFalse(outsideRangePredicate.evaluate(2.5f));
    assertTrue(outsideRangePredicate.evaluate(3.5f));
  }

  @Test
  public void testDoublePredicates() {
    double epsilon = 1e-15;

    // Basic equality with epsilon
    assertTrue(DoublePredicate.equalTo(1.0, epsilon).evaluate(1.0));
    assertTrue(DoublePredicate.equalTo(1.0, epsilon).evaluate(1.0 + epsilon / 2));
    assertFalse(DoublePredicate.equalTo(1.0, epsilon).evaluate(1.0 + epsilon * 2));

    // Basic comparisons
    assertTrue(DoublePredicate.greaterThan(1.0).evaluate(2.0));
    assertFalse(DoublePredicate.greaterThan(1.0).evaluate(1.0));

    assertTrue(DoublePredicate.greaterOrEquals(1.0).evaluate(1.0));
    assertFalse(DoublePredicate.greaterOrEquals(2.0).evaluate(1.0));

    assertTrue(DoublePredicate.lowerThan(2.0).evaluate(1.0));
    assertFalse(DoublePredicate.lowerThan(1.0).evaluate(1.0));

    assertTrue(DoublePredicate.lowerOrEquals(1.0).evaluate(1.0));
    assertFalse(DoublePredicate.lowerOrEquals(1.0).evaluate(2.0));

    // AnyOf predicate
    DoublePredicate anyOf = DoublePredicate.anyOf(3.0, 5.0, 10.0);
    assertFalse(anyOf.evaluate(1.0));
    assertFalse(anyOf.evaluate(2.0));
    assertTrue(anyOf.evaluate(3.0));
    assertFalse(anyOf.evaluate(4.0));
    assertTrue(anyOf.evaluate(5.0));
    assertFalse(anyOf.evaluate(6.0));
    assertFalse(anyOf.evaluate(7.0));
    assertFalse(anyOf.evaluate(8.0));
    assertFalse(anyOf.evaluate(9.0));
    assertTrue(anyOf.evaluate(10.0));
    assertFalse(anyOf.evaluate(11.0));

    // Special values tests
    assertTrue(DoublePredicate.equalTo(Double.NaN, epsilon).evaluate(Double.NaN));
    assertTrue(DoublePredicate.equalTo(Double.POSITIVE_INFINITY, epsilon).evaluate(Double.POSITIVE_INFINITY));
    assertTrue(DoublePredicate.equalTo(Double.NEGATIVE_INFINITY, epsilon).evaluate(Double.NEGATIVE_INFINITY));

    // Null handling
    assertFalse(DoublePredicate.equalTo(1.0, epsilon).evaluate(null));
    assertFalse(DoublePredicate.greaterThan(1.0).evaluate(null));
    assertFalse(DoublePredicate.anyOf(1.0, 2.0).evaluate(null));

    // Composition tests
    Predicate<Double> betweenPredicate =
        Predicate.and(DoublePredicate.greaterThan(1.0), DoublePredicate.lowerOrEquals(3.0));
    assertFalse(betweenPredicate.evaluate(1.0));
    assertTrue(betweenPredicate.evaluate(2.0));
    assertTrue(betweenPredicate.evaluate(3.0));
    assertFalse(betweenPredicate.evaluate(4.0));

    Predicate<Double> outsideRangePredicate =
        Predicate.or(DoublePredicate.lowerThan(1.0), DoublePredicate.greaterThan(3.0));
    assertTrue(outsideRangePredicate.evaluate(0.5));
    assertFalse(outsideRangePredicate.evaluate(1.5));
    assertFalse(outsideRangePredicate.evaluate(2.5));
    assertTrue(outsideRangePredicate.evaluate(3.5));
  }

  @Test
  public void testFloatDoublePredicatesEdgeCases() {
    float fepsilon = 1e-6f;
    double depsilon = 1e-15;

    // Test NaN equality with different NaN representations
    assertTrue(FloatPredicate.equalTo(Float.NaN, fepsilon).evaluate(Float.NaN));
    assertTrue(FloatPredicate.equalTo(-Float.NaN, fepsilon).evaluate(Float.NaN));
    assertTrue(FloatPredicate.equalTo(Float.NaN, fepsilon).evaluate(-Float.NaN));

    assertTrue(DoublePredicate.equalTo(Double.NaN, depsilon).evaluate(Double.NaN));
    assertTrue(DoublePredicate.equalTo(-Double.NaN, depsilon).evaluate(Double.NaN));
    assertTrue(DoublePredicate.equalTo(Double.NaN, depsilon).evaluate(-Double.NaN));

    // Test infinity comparisons
    assertTrue(FloatPredicate.equalTo(Float.POSITIVE_INFINITY, fepsilon).evaluate(Float.POSITIVE_INFINITY));
    assertTrue(FloatPredicate.equalTo(Float.NEGATIVE_INFINITY, fepsilon).evaluate(Float.NEGATIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(Float.POSITIVE_INFINITY, fepsilon).evaluate(Float.NEGATIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(Float.NEGATIVE_INFINITY, fepsilon).evaluate(Float.POSITIVE_INFINITY));

    assertTrue(DoublePredicate.equalTo(Double.POSITIVE_INFINITY, depsilon).evaluate(Double.POSITIVE_INFINITY));
    assertTrue(DoublePredicate.equalTo(Double.NEGATIVE_INFINITY, depsilon).evaluate(Double.NEGATIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(Double.POSITIVE_INFINITY, depsilon).evaluate(Double.NEGATIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(Double.NEGATIVE_INFINITY, depsilon).evaluate(Double.POSITIVE_INFINITY));

    // Test mixed infinity and finite number comparisons
    assertFalse(FloatPredicate.equalTo(Float.POSITIVE_INFINITY, fepsilon).evaluate(Float.MAX_VALUE));
    assertFalse(FloatPredicate.equalTo(Float.NEGATIVE_INFINITY, fepsilon).evaluate(-Float.MAX_VALUE));
    assertFalse(FloatPredicate.equalTo(Float.MAX_VALUE, fepsilon).evaluate(Float.POSITIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(-Float.MAX_VALUE, fepsilon).evaluate(Float.NEGATIVE_INFINITY));

    assertFalse(DoublePredicate.equalTo(Double.POSITIVE_INFINITY, depsilon).evaluate(Double.MAX_VALUE));
    assertFalse(DoublePredicate.equalTo(Double.NEGATIVE_INFINITY, depsilon).evaluate(-Double.MAX_VALUE));
    assertFalse(DoublePredicate.equalTo(Double.MAX_VALUE, depsilon).evaluate(Double.POSITIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(-Double.MAX_VALUE, depsilon).evaluate(Double.NEGATIVE_INFINITY));

    // Test NaN with infinity comparisons
    assertFalse(FloatPredicate.equalTo(Float.NaN, fepsilon).evaluate(Float.POSITIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(Float.NaN, fepsilon).evaluate(Float.NEGATIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(Float.POSITIVE_INFINITY, fepsilon).evaluate(Float.NaN));
    assertFalse(FloatPredicate.equalTo(Float.NEGATIVE_INFINITY, fepsilon).evaluate(Float.NaN));

    assertFalse(DoublePredicate.equalTo(Double.NaN, depsilon).evaluate(Double.POSITIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(Double.NaN, depsilon).evaluate(Double.NEGATIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(Double.POSITIVE_INFINITY, depsilon).evaluate(Double.NaN));
    assertFalse(DoublePredicate.equalTo(Double.NEGATIVE_INFINITY, depsilon).evaluate(Double.NaN));

    // Test epsilon behavior near special values
    assertFalse(FloatPredicate.equalTo(Float.MAX_VALUE, fepsilon).evaluate(Float.POSITIVE_INFINITY));
    assertFalse(FloatPredicate.equalTo(-Float.MAX_VALUE, fepsilon).evaluate(Float.NEGATIVE_INFINITY));

    assertFalse(DoublePredicate.equalTo(Double.MAX_VALUE, depsilon).evaluate(Double.POSITIVE_INFINITY));
    assertFalse(DoublePredicate.equalTo(-Double.MAX_VALUE, depsilon).evaluate(Double.NEGATIVE_INFINITY));

    // Test exact equality vs epsilon equality
    float f1 = 1.0f;
    float f2 = 1.0f;
    float f3 = Float.intBitsToFloat(Float.floatToIntBits(1.0f) + 1); // Next representable float after 1.0

    double d1 = 1.0;
    double d2 = 1.0;
    double d3 = Double.longBitsToDouble(Double.doubleToLongBits(1.0) + 1); // Next representable double after 1.0

    assertTrue(FloatPredicate.equalTo(f1, fepsilon).evaluate(f2)); // Exact equality
    assertTrue(FloatPredicate.equalTo(f1, fepsilon).evaluate(f3)); // Within epsilon

    assertTrue(DoublePredicate.equalTo(d1, depsilon).evaluate(d2)); // Exact equality
    assertTrue(DoublePredicate.equalTo(d1, depsilon).evaluate(d3)); // Within epsilon
  }
}
