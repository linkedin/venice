package com.linkedin.venice.client.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


/**
 * Unit tests for QueryTool functionality including facet counting features.
 */
public class QueryToolTest {
  @Test
  public void testRemoveQuotes() {
    // Test basic quote removal
    assertEquals(QueryTool.removeQuotes("\"test\""), "test");
    assertEquals(QueryTool.removeQuotes("test"), "test");
    assertEquals(QueryTool.removeQuotes("\"test"), "test");
    assertEquals(QueryTool.removeQuotes("test\""), "test");
    assertEquals(QueryTool.removeQuotes("\"\""), "");
    assertEquals(QueryTool.removeQuotes(""), "");
    // Test edge cases
    assertEquals(QueryTool.removeQuotes("\"test\"test\""), "test\"test");
    assertEquals(QueryTool.removeQuotes("\"test\"test"), "test\"test");
  }

  @Test
  public void testConvertKey() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    // Test string conversion
    Object stringKey = QueryTool.convertKey("test", stringSchema);
    assertEquals(stringKey, "test");
    assertEquals(stringKey.getClass(), String.class);

    // Test int conversion
    Object intKey = QueryTool.convertKey("123", intSchema);
    assertEquals(intKey, 123);
    assertEquals(intKey.getClass(), Integer.class);

    // Test long conversion
    Object longKey = QueryTool.convertKey("123456789", longSchema);
    assertEquals(longKey, 123456789L);
    assertEquals(longKey.getClass(), Long.class);

    // Test float conversion
    Object floatKey = QueryTool.convertKey("123.45", floatSchema);
    assertEquals(floatKey, 123.45f);
    assertEquals(floatKey.getClass(), Float.class);

    // Test double conversion
    Object doubleKey = QueryTool.convertKey("123.456", doubleSchema);
    assertEquals(doubleKey, 123.456);
    assertEquals(doubleKey.getClass(), Double.class);

    // Test boolean conversion
    Object booleanKey = QueryTool.convertKey("true", booleanSchema);
    assertEquals(booleanKey, true);
    assertEquals(booleanKey.getClass(), Boolean.class);
  }

  @Test
  public void testConvertKeyInvalidInput() {
    Schema intSchema = Schema.create(Schema.Type.INT);

    // Test invalid integer
    VeniceException exception =
        expectThrows(VeniceException.class, () -> QueryTool.convertKey("not_a_number", intSchema));
    assertTrue(exception.getMessage().contains("Invalid number format for key: not_a_number"));
  }

  @Test
  public void testConvertKeyInvalidInputForLong() {
    Schema longSchema = Schema.create(Schema.Type.LONG);

    // Test invalid long
    VeniceException exception =
        expectThrows(VeniceException.class, () -> QueryTool.convertKey("not_a_number", longSchema));
    assertTrue(exception.getMessage().contains("Invalid number format for key: not_a_number"));
  }

  @Test
  public void testConvertKeyInvalidInputForFloat() {
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);

    // Test invalid float
    VeniceException exception =
        expectThrows(VeniceException.class, () -> QueryTool.convertKey("not_a_number", floatSchema));
    assertTrue(exception.getMessage().contains("Invalid number format for key: not_a_number"));
  }

  @Test
  public void testConvertKeyInvalidInputForDouble() {
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);

    // Test invalid double
    VeniceException exception =
        expectThrows(VeniceException.class, () -> QueryTool.convertKey("not_a_number", doubleSchema));
    assertTrue(exception.getMessage().contains("Invalid number format for key: not_a_number"));
  }

  @Test
  public void testConvertKeyBooleanValues() {
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    // Test true boolean
    Object trueKey = QueryTool.convertKey("true", booleanSchema);
    assertEquals(trueKey, true);
    assertEquals(trueKey.getClass(), Boolean.class);

    // Test false boolean
    Object falseKey = QueryTool.convertKey("false", booleanSchema);
    assertEquals(falseKey, false);
    assertEquals(falseKey.getClass(), Boolean.class);
  }

  @Test
  public void testConvertKeyEdgeCases() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema intSchema = Schema.create(Schema.Type.INT);

    // Test empty string
    Object emptyKey = QueryTool.convertKey("", stringSchema);
    assertEquals(emptyKey, "");

    // Test zero values
    Object zeroInt = QueryTool.convertKey("0", intSchema);
    assertEquals(zeroInt, 0);

    // Test negative values
    Object negativeInt = QueryTool.convertKey("-123", intSchema);
    assertEquals(negativeInt, -123);
  }

  @Test
  public void testParseBucketDefinitions() {
    // Test range format
    Map<String, Predicate<Long>> rangePredicates = QueryTool.parseBucketDefinitions("20-25,30-35");
    assertNotNull(rangePredicates);
    assertEquals(rangePredicates.size(), 2);
    assertTrue(rangePredicates.containsKey("20-25"));
    assertTrue(rangePredicates.containsKey("30-35"));

    // Test operator format
    Map<String, Predicate<Long>> operatorPredicates = QueryTool.parseBucketDefinitions("young:lt:30,senior:gte:30");
    assertNotNull(operatorPredicates);
    assertEquals(operatorPredicates.size(), 2);
    assertTrue(operatorPredicates.containsKey("young"));
    assertTrue(operatorPredicates.containsKey("senior"));

    // Test mixed format
    Map<String, Predicate<Long>> mixedPredicates = QueryTool.parseBucketDefinitions("20-25,senior:gte:30");
    assertNotNull(mixedPredicates);
    assertEquals(mixedPredicates.size(), 2);
    assertTrue(mixedPredicates.containsKey("20-25"));
    assertTrue(mixedPredicates.containsKey("senior"));
  }

  @Test
  public void testParseBucketDefinitionsInvalidFormat() {
    // Test invalid range format
    VeniceException rangeException =
        expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("20-25-30"));
    assertTrue(rangeException.getMessage().contains("Invalid range format"));

    // Test invalid operator format
    VeniceException operatorException =
        expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("bucket:invalid:30"));
    assertTrue(operatorException.getMessage().contains("Unknown operator"));

    // Test malformed operator
    VeniceException malformedException =
        expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("bucket:lt"));
    assertTrue(malformedException.getMessage().contains("Invalid bucket definition format"));
  }

  @Test
  public void testParseBucketDefinitionsEmpty() {
    // Test null input
    Map<String, Predicate<Long>> nullPredicates = QueryTool.parseBucketDefinitions(null);
    assertNotNull(nullPredicates);
    assertEquals(nullPredicates.size(), 0);

    // Test empty string
    Map<String, Predicate<Long>> emptyPredicates = QueryTool.parseBucketDefinitions("");
    assertNotNull(emptyPredicates);
    assertEquals(emptyPredicates.size(), 0);
  }

  @Test
  public void testParseBucketDefinitionsOperators() {
    Map<String, Predicate<Long>> predicates =
        QueryTool.parseBucketDefinitions("lt:lt:10,lte:lte:20,gt:gt:30,gte:gte:40,eq:eq:50");

    assertNotNull(predicates);
    assertEquals(predicates.size(), 5);

    // Verify each operator creates the correct predicate type
    assertTrue(predicates.containsKey("lt"));
    assertTrue(predicates.containsKey("lte"));
    assertTrue(predicates.containsKey("gt"));
    assertTrue(predicates.containsKey("gte"));
    assertTrue(predicates.containsKey("eq"));
  }

  @Test
  public void testParseKeys() {
    // This test would require mocking the AvroGenericStoreClient
    // For now, we'll test the logic separately
    String keyString = "key1,key2,key3";
    String[] keyStrings = keyString.split(",");

    assertEquals(keyStrings.length, 3);
    assertEquals(keyStrings[0].trim(), "key1");
    assertEquals(keyStrings[1].trim(), "key2");
    assertEquals(keyStrings[2].trim(), "key3");
  }

  @Test
  public void testParseKeysSingleKey() {
    String keyString = "single_key";
    String[] keyStrings = keyString.split(",");

    assertEquals(keyStrings.length, 1);
    assertEquals(keyStrings[0].trim(), "single_key");
  }

  @Test
  public void testParseKeysWithSpaces() {
    String keyString = " key1 , key2 , key3 ";
    String[] keyStrings = keyString.split(",");

    assertEquals(keyStrings.length, 3);
    assertEquals(keyStrings[0].trim(), "key1");
    assertEquals(keyStrings[1].trim(), "key2");
    assertEquals(keyStrings[2].trim(), "key3");
  }

  @Test
  public void testParseBucketDefinitionsRangeFormat() {
    // Test simple range
    Map<String, Predicate<Long>> predicates = QueryTool.parseBucketDefinitions("10-20");
    assertNotNull(predicates);
    assertEquals(predicates.size(), 1);
    assertTrue(predicates.containsKey("10-20"));

    // Test multiple ranges
    Map<String, Predicate<Long>> multiPredicates = QueryTool.parseBucketDefinitions("0-10,20-30,40-50");
    assertNotNull(multiPredicates);
    assertEquals(multiPredicates.size(), 3);
    assertTrue(multiPredicates.containsKey("0-10"));
    assertTrue(multiPredicates.containsKey("20-30"));
    assertTrue(multiPredicates.containsKey("40-50"));
  }

  @Test
  public void testParseBucketDefinitionsRangeFormatInvalid() {
    // Test invalid range format
    VeniceException exception = expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("10-20-30"));
    assertTrue(exception.getMessage().contains("Invalid range format"));

    // Test non-numeric range
    VeniceException numberException =
        expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("abc-def"));
    assertTrue(numberException.getMessage().contains("Invalid number format in range: abc-def"));
  }

  @Test
  public void testParseBucketDefinitionsOperatorFormat() {
    // Test all operators
    Map<String, Predicate<Long>> predicates =
        QueryTool.parseBucketDefinitions("less:lt:10,lessEqual:lte:20,greater:gt:30,greaterEqual:gte:40,equal:eq:50");

    assertNotNull(predicates);
    assertEquals(predicates.size(), 5);
    assertTrue(predicates.containsKey("less"));
    assertTrue(predicates.containsKey("lessEqual"));
    assertTrue(predicates.containsKey("greater"));
    assertTrue(predicates.containsKey("greaterEqual"));
    assertTrue(predicates.containsKey("equal"));
  }

  @Test
  public void testParseBucketDefinitionsMixedFormat() {
    // Test mixing range and operator formats
    Map<String, Predicate<Long>> predicates = QueryTool.parseBucketDefinitions("0-10,young:lt:30,20-25,senior:gte:50");

    assertNotNull(predicates);
    assertEquals(predicates.size(), 4);
    assertTrue(predicates.containsKey("0-10"));
    assertTrue(predicates.containsKey("young"));
    assertTrue(predicates.containsKey("20-25"));
    assertTrue(predicates.containsKey("senior"));
  }

  @Test
  public void testParseBucketDefinitionsNumberFormatException() {
    // Test invalid number format in bucket definition
    VeniceException exception =
        expectThrows(VeniceException.class, () -> QueryTool.parseBucketDefinitions("bucket:lt:abc"));
    assertTrue(exception.getMessage().contains("Invalid number format in bucket definition"));
  }

  @Test
  public void testParseBucketDefinitionsWithSpaces() {
    // Test bucket definitions with spaces
    Map<String, Predicate<Long>> predicates = QueryTool.parseBucketDefinitions(" 20-25 , 30-35 ");
    assertNotNull(predicates);
    assertEquals(predicates.size(), 2);
    assertTrue(predicates.containsKey("20-25"));
    assertTrue(predicates.containsKey("30-35"));
  }

  @Test
  public void testParseBucketDefinitionsSingleRange() {
    // Test single range
    Map<String, Predicate<Long>> predicates = QueryTool.parseBucketDefinitions("10-20");
    assertNotNull(predicates);
    assertEquals(predicates.size(), 1);
    assertTrue(predicates.containsKey("10-20"));
  }

  @Test
  public void testParseBucketDefinitionsSingleOperator() {
    // Test single operator
    Map<String, Predicate<Long>> predicates = QueryTool.parseBucketDefinitions("young:lt:30");
    assertNotNull(predicates);
    assertEquals(predicates.size(), 1);
    assertTrue(predicates.containsKey("young"));
  }

  @Test
  public void testParseBucketDefinitionsComplexMixed() {
    // Test complex mixed format
    Map<String, Predicate<Long>> predicates =
        QueryTool.parseBucketDefinitions("0-10,young:lt:30,20-25,senior:gte:50,elder:gt:70");
    assertNotNull(predicates);
    assertEquals(predicates.size(), 5);
    assertTrue(predicates.containsKey("0-10"));
    assertTrue(predicates.containsKey("young"));
    assertTrue(predicates.containsKey("20-25"));
    assertTrue(predicates.containsKey("senior"));
    assertTrue(predicates.containsKey("elder"));
  }

  @Test
  public void testMainMethodWithInsufficientArgs() throws Exception {
    // Test main method with insufficient arguments
    String[] args = { "store", "key" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testMainMethodWithValidArgs() throws Exception {
    // Test main method with valid arguments for single mode
    String[] args = { "store", "key", "url", "false", "ssl.config", "single" };
    int exitCode = QueryTool.run(args);
    // Expected to fail due to missing client setup, returns 1
    assertEquals(exitCode, 1);
  }

  @Test
  public void testMainMethodWithCountByValueMode() throws Exception {
    // Test main method with countByValue mode
    String[] args = { "store", "key", "url", "false", "ssl.config", "countByValue", "field1", "5" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testMainMethodWithCountByBucketMode() throws Exception {
    // Test main method with countByBucket mode
    String[] args = { "store", "key", "url", "false", "ssl.config", "countByBucket", "field1", "bucket:lt:30" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testMainMethodWithUnknownMode() throws Exception {
    // Test main method with unknown facet counting mode
    String[] args = { "store", "key", "url", "false", "ssl.config", "unknown" };
    // Expect IllegalArgumentException to be thrown and caught, returning exit code 1
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testConvertKeyWithUnionSchema() {
    Schema unionSchema = Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT));
    // Only test cases that cannot be parsed, assert AvroTypeException is thrown
    assertThrows(org.apache.avro.AvroTypeException.class, () -> QueryTool.convertKey("123", unionSchema));
    assertThrows(org.apache.avro.AvroTypeException.class, () -> QueryTool.convertKey("\"abc\"", unionSchema));
    assertThrows(org.apache.avro.AvroTypeException.class, () -> QueryTool.convertKey("true", unionSchema));
  }

  @Test
  public void testConvertKeyWithComplexSchema() {
    // Record type cannot be directly deserialized, just ensure result is not null
    Schema recordSchema = Schema.createRecord("TestRecord", null, null, false);
    recordSchema.setFields(java.util.Collections.emptyList());
    Object result = QueryTool.convertKey("{}", recordSchema);
    assertNotNull(result);
  }

  @Test
  public void testConvertKeyWithAllNumericTypes() {
    // Test convertKey with all numeric types
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);

    // Test negative values
    assertEquals(QueryTool.convertKey("-123", intSchema), -123);
    assertEquals(QueryTool.convertKey("-123456789", longSchema), -123456789L);
    assertEquals(QueryTool.convertKey("-123.45", floatSchema), -123.45f);
    assertEquals(QueryTool.convertKey("-123.456", doubleSchema), -123.456);

    // Test zero values
    assertEquals(QueryTool.convertKey("0", intSchema), 0);
    assertEquals(QueryTool.convertKey("0", longSchema), 0L);
    assertEquals(QueryTool.convertKey("0.0", floatSchema), 0.0f);
    assertEquals(QueryTool.convertKey("0.0", doubleSchema), 0.0);
  }

  @Test
  public void testConvertKeyWithBooleanEdgeCases() {
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    // Test various boolean representations
    assertEquals(QueryTool.convertKey("true", booleanSchema), true);
    assertEquals(QueryTool.convertKey("false", booleanSchema), false);
    assertEquals(QueryTool.convertKey("TRUE", booleanSchema), true);
    assertEquals(QueryTool.convertKey("FALSE", booleanSchema), false);
  }

  @Test
  public void testConvertKeyWithStringEdgeCases() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // Test empty string
    assertEquals(QueryTool.convertKey("", stringSchema), "");

    // Test string with special characters
    assertEquals(QueryTool.convertKey("test123", stringSchema), "test123");
    assertEquals(QueryTool.convertKey("test-123", stringSchema), "test-123");
    assertEquals(QueryTool.convertKey("test_123", stringSchema), "test_123");
  }

  @Test
  public void testConvertKeyWithLargeNumbers() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);

    // Test large numbers
    assertEquals(QueryTool.convertKey("9223372036854775807", longSchema), 9223372036854775807L);
    assertEquals(QueryTool.convertKey("3.141592653589793", doubleSchema), Math.PI);
  }
}
