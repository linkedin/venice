package com.linkedin.venice.client.store.predicate;

import static com.linkedin.venice.client.store.predicate.PredicateBuilder.and;
import static com.linkedin.venice.client.store.predicate.PredicateBuilder.equalTo;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PredicateTest {
  private static final String STRING_FIELD_VALUE = "helloWorld";
  private static final int INT_FIELD_VALUE = 123456;
  private static final float[] FLOAT_ARRAY_FIELD_VALUE = { 0.0f, 1.1f, 2.2f, 3.3f, 4.4f };
  private static final Schema KEY_SCHEMA = new Schema.Parser().parse(
      "{" + "  \"namespace\": \"example.predicate\",    " + "  \"type\": \"record\",        "
          + "  \"name\": \"KeyRecord\",       " + "  \"fields\": [        "
          + "         { \"name\": \"stringField\", \"type\": \"string\" },             "
          + "         { \"name\": \"intField\", \"type\": \"int\" },           "
          + "         {   \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  } "
          + "  ]       " + " }       ");

  @Test
  public void testPredicatesAllMatchingFields() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate predicateToTest = and(
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

    Predicate predicateToTest = equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesMisMatchedArrayElement() {
    float[] expectedFloatArrayFieldValue = { 0.0f, 1.1f, 2.2f, 3.3f, 5.5f };

    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", expectedFloatArrayFieldValue));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesNullRecordToTest() {
    Predicate predicateToTest = and(
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

    Predicate predicateToTest =
        and(equalTo("stringField", STRING_FIELD_VALUE), equalTo("nonExistentField", "fakeValue"));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesNullExpectedFields() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", null);
    keyRecord.put("intField", null);
    keyRecord.put("floatArrayField", null);

    Predicate predicateToTest =
        and(equalTo("stringField", null), equalTo("intField", null), equalTo("floatArrayField", null));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesUnexpectedNullField() {
    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", null);
    keyRecord.put("intField", null);
    keyRecord.put("floatArrayField", null);

    Predicate predicateToTest = equalTo("stringField", STRING_FIELD_VALUE);

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesDeepEquals() {
    String stringFieldValueCopy = "helloWorld";
    int intFieldValueCopy = 123456;
    float[] floatArrayFieldValueCopy = FLOAT_ARRAY_FIELD_VALUE.clone();

    GenericRecord keyRecord = new GenericData.Record(KEY_SCHEMA);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);

    Predicate predicateToTest = and(
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

    Predicate predicateToTest = and(
        and(equalTo("stringField", STRING_FIELD_VALUE)),
        and(),
        and(equalTo("intField", INT_FIELD_VALUE), equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE)));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
  }

  @Test
  public void testPredicatesMatchingNestedFields() {

    String nestedRecordSchemaString = "{ " + "                         \"type\" : \"record\", \n"
        + "                         \"name\" : \"KeyRecord2\",\n" + "                         \"fields\" : [\n"
        + "                            {\"name\" : \"stringField\", \"type\" : \"string\"}, \n"
        + "                            {\"name\" : \"booleanField\", \"type\" : \"boolean\"} ]}";

    Schema nestedRecordKeySchema = new Schema.Parser().parse(nestedRecordSchemaString);

    Schema fullRecordKeySchema = new Schema.Parser().parse(
        "{" + "  \"namespace\": \"example.predicate\",    " + "  \"type\": \"record\",        "
            + "  \"name\": \"KeyRecord\",       " + "  \"fields\": [        "
            + "         { \"name\": \"stringField\", \"type\": \"string\" },             "
            + "         { \"name\": \"intField\", \"type\": \"int\" },           "
            + "         { \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }, "
            + "         { \"name\": \"nestedRecord\", \"type\": " + nestedRecordSchemaString + "}  ]       "
            + " }       ");

    GenericRecord nestedRecord = new GenericData.Record(nestedRecordKeySchema);
    nestedRecord.put("stringField", STRING_FIELD_VALUE);
    nestedRecord.put("booleanField", true);

    GenericRecord keyRecord = new GenericData.Record(fullRecordKeySchema);
    keyRecord.put("stringField", STRING_FIELD_VALUE);
    keyRecord.put("intField", INT_FIELD_VALUE);
    keyRecord.put("floatArrayField", FLOAT_ARRAY_FIELD_VALUE);
    keyRecord.put("nestedRecord", nestedRecord);

    Predicate predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE),
        equalTo("nestedRecord", nestedRecord));

    Predicate nestedPredicateToTest = and(equalTo("stringField", STRING_FIELD_VALUE), equalTo("booleanField", true));

    Assert.assertTrue(predicateToTest.evaluate(keyRecord));
    Assert.assertTrue(nestedPredicateToTest.evaluate((GenericRecord) keyRecord.get("nestedRecord")));
  }

  @Test
  public void testPredicatesMisMatchedNestedFields() {

    String nestedRecordSchemaString = "{ " + "                         \"type\" : \"record\", \n"
        + "                         \"name\" : \"KeyRecord2\",\n" + "                         \"fields\" : [\n"
        + "                            {\"name\" : \"stringField\", \"type\" : \"string\"}, \n"
        + "                            {\"name\" : \"booleanField\", \"type\" : \"boolean\"} ]}";

    Schema nestedRecordKeySchema = new Schema.Parser().parse(nestedRecordSchemaString);

    Schema fullRecordKeySchema = new Schema.Parser().parse(
        "{" + "  \"namespace\": \"example.predicate\",    " + "  \"type\": \"record\",        "
            + "  \"name\": \"KeyRecord\",       " + "  \"fields\": [        "
            + "         { \"name\": \"stringField\", \"type\": \"string\" },             "
            + "         { \"name\": \"intField\", \"type\": \"int\" },           "
            + "         { \"default\": [], \n  \"name\": \"floatArrayField\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }, "
            + "         { \"name\": \"nestedRecord\", \"type\": " + nestedRecordSchemaString + "}  ]       "
            + " }       ");

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

    Predicate predicateToTest = and(
        equalTo("stringField", STRING_FIELD_VALUE),
        equalTo("intField", INT_FIELD_VALUE),
        equalTo("floatArrayField", FLOAT_ARRAY_FIELD_VALUE),
        equalTo("nestedRecord", expectedNestedRecord));

    Predicate nestedPredicateToTest = and(equalTo("stringField", "mismatched string"), equalTo("booleanField", false));

    Assert.assertFalse(predicateToTest.evaluate(keyRecord));
    Assert.assertFalse(nestedPredicateToTest.evaluate((GenericRecord) keyRecord.get("nestedRecord")));
  }

  @Test
  public void testPredicateCreationWithNullFieldName() {
    Assert.assertThrows(VeniceClientException.class, () -> and(equalTo(null, null)));
  }
}
