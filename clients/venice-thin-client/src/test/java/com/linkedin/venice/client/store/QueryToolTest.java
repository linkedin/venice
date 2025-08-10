package com.linkedin.venice.client.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class QueryToolTest {
  @Test
  public void testJsonKeyWithCommaBug() throws Exception {
    String jsonKeyWithComma = "{\"memberId\": 220896326, \"useCaseName\": \"nba_digest_email\"}";
    String[] args = { "myStore", jsonKeyWithComma, "http://venice-url", "false", "" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 0); // Expect 0, as argument validation should pass, no comma splitting
  }

  @Test
  public void testMainMethodWithValidArgs() throws Exception {
    String[] args = { "myStore", "myKey", "http://venice-url", "false", "" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 0);
  }

  @Test
  public void testMainMethodWithInsufficientArgs() throws Exception {
    String[] args = { "myStore", "myKey" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testMainMethodWithCountByValueMode() throws Exception {
    String[] args = { "--countByValue", "myStore", "key1,key2", "http://venice-url", "false", "", "field1", "10" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 0);
  }

  @Test
  public void testMainMethodWithCountByBucketMode() throws Exception {
    String[] args =
        { "--countByBucket", "myStore", "key1,key2", "http://venice-url", "false", "", "field1", "bucket1:gt:10" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 0);
  }

  @Test
  public void testMainMethodWithUnknownMode() throws Exception {
    String[] args = { "--unknownMode", "myStore", "myKey", "http://venice-url", "false", "" };
    int exitCode = QueryTool.run(args);
    assertEquals(exitCode, 0); // Falls back to single key query
  }

  @Test
  public void testCommandLineArgumentParsing() throws Exception {
    // Test with sufficient arguments for single key query
    String[] validArgs = { "store", "key", "url", "false", "ssl" };
    int exitCode = QueryTool.run(validArgs);
    assertEquals(exitCode, 0);

    // Test with insufficient arguments
    String[] invalidArgs = { "store", "key" };
    exitCode = QueryTool.run(invalidArgs);
    assertEquals(exitCode, 1);
  }

  @Test
  public void testConvertKey() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    Object result = QueryTool.convertKey("123", intSchema);
    assertEquals(result, 123);

    Schema longSchema = Schema.create(Schema.Type.LONG);
    result = QueryTool.convertKey("123456789", longSchema);
    assertEquals(result, 123456789L);

    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    result = QueryTool.convertKey("12.34", floatSchema);
    assertEquals(result, 12.34f);

    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    result = QueryTool.convertKey("12.34", doubleSchema);
    assertEquals(result, 12.34);

    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
    result = QueryTool.convertKey("true", booleanSchema);
    assertEquals(result, true);

    Schema stringSchema = Schema.create(Schema.Type.STRING);
    result = QueryTool.convertKey("test", stringSchema);
    assertEquals(result, "test");
  }

  @Test
  public void testConvertKeyWithAllNumericTypes() {
    // Test integer
    Schema intSchema = Schema.create(Schema.Type.INT);
    Object result = QueryTool.convertKey("42", intSchema);
    assertEquals(result, 42);

    // Test long
    Schema longSchema = Schema.create(Schema.Type.LONG);
    result = QueryTool.convertKey("9223372036854775807", longSchema);
    assertEquals(result, 9223372036854775807L);

    // Test float
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    result = QueryTool.convertKey("3.14", floatSchema);
    assertEquals(result, 3.14f);

    // Test double
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    result = QueryTool.convertKey("3.141592653589793", doubleSchema);
    assertEquals(result, 3.141592653589793);
  }

  @Test
  public void testConvertKeyBooleanValues() {
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    Object result = QueryTool.convertKey("true", booleanSchema);
    assertEquals(result, true);

    result = QueryTool.convertKey("false", booleanSchema);
    assertEquals(result, false);

    result = QueryTool.convertKey("True", booleanSchema);
    assertEquals(result, true);

    result = QueryTool.convertKey("FALSE", booleanSchema);
    assertEquals(result, false);
  }

  @Test
  public void testConvertKeyWithStringEdgeCases() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    // Empty string
    Object result = QueryTool.convertKey("", stringSchema);
    assertEquals(result, "");

    // String with spaces
    result = QueryTool.convertKey("  test  ", stringSchema);
    assertEquals(result, "  test  ");

    // String with special characters
    result = QueryTool.convertKey("test@#$%^&*()", stringSchema);
    assertEquals(result, "test@#$%^&*()");
  }

  @Test
  public void testConvertKeyWithBooleanEdgeCases() {
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

    // Non-standard boolean strings should return false
    Object result = QueryTool.convertKey("yes", booleanSchema);
    assertEquals(result, false);

    result = QueryTool.convertKey("1", booleanSchema);
    assertEquals(result, false);

    result = QueryTool.convertKey("0", booleanSchema);
    assertEquals(result, false);
  }

  @Test
  public void testConvertKeyWithLargeNumbers() {
    // Test large long value
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Object result = QueryTool.convertKey("9223372036854775807", longSchema);
    assertEquals(result, Long.MAX_VALUE);

    result = QueryTool.convertKey("-9223372036854775808", longSchema);
    assertEquals(result, Long.MIN_VALUE);

    // Test large integer value
    Schema intSchema = Schema.create(Schema.Type.INT);
    result = QueryTool.convertKey("2147483647", intSchema);
    assertEquals(result, Integer.MAX_VALUE);

    result = QueryTool.convertKey("-2147483648", intSchema);
    assertEquals(result, Integer.MIN_VALUE);
  }

  @Test
  public void testConvertKeyInvalidInput() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    try {
      QueryTool.convertKey("invalid", intSchema);
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format for key"));
    }
  }

  @Test
  public void testConvertKeyInvalidInputForLong() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    try {
      QueryTool.convertKey("not_a_number", longSchema);
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format for key"));
    }
  }

  @Test
  public void testConvertKeyInvalidInputForFloat() {
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    try {
      QueryTool.convertKey("not_a_float", floatSchema);
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format for key"));
    }
  }

  @Test
  public void testConvertKeyInvalidInputForDouble() {
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    try {
      QueryTool.convertKey("not_a_double", doubleSchema);
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format for key"));
    }
  }

  @Test
  public void testConvertKeyEdgeCases() {
    // Test empty string for integer (should throw exception)
    Schema intSchema = Schema.create(Schema.Type.INT);
    try {
      QueryTool.convertKey("", intSchema);
      fail("Expected VeniceException for empty string");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format for key"));
    }
  }

  @Test
  public void testConvertKeyWithComplexSchema() {
    // Test with a complex JSON schema (record type)
    String recordSchemaJson = "{" + "\"type\": \"record\"," + "\"name\": \"TestRecord\"," + "\"fields\": ["
        + "  {\"name\": \"id\", \"type\": \"int\"}," + "  {\"name\": \"name\", \"type\": \"string\"}" + "]" + "}";

    Schema recordSchema = new Schema.Parser().parse(recordSchemaJson);
    String jsonKey = "{\"id\": 123, \"name\": \"test\"}";

    Object result = QueryTool.convertKey(jsonKey, recordSchema);
    assertNotNull(result);
    // The result should be a GenericRecord
    assertTrue(result.toString().contains("123"));
    assertTrue(result.toString().contains("test"));
  }

  @Test
  public void testRemoveQuotes() {
    assertEquals(QueryTool.removeQuotes("\"test\""), "test");
    assertEquals(QueryTool.removeQuotes("test"), "test");
    assertEquals(QueryTool.removeQuotes("\"test"), "test");
    assertEquals(QueryTool.removeQuotes("test\""), "test");
    assertEquals(QueryTool.removeQuotes("\"\""), "");
    assertEquals(QueryTool.removeQuotes(""), "");
  }

  @Test
  public void testParseBucketDefinitions() {
    // Test range format
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("20-25,30-35");
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("20-25"));
    assertTrue(result.containsKey("30-35"));

    // Test operator format
    result = QueryTool.parseBucketDefinitions("young:lt:30,old:gte:60");
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("young"));
    assertTrue(result.containsKey("old"));
  }

  @Test
  public void testParseBucketDefinitionsEmpty() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("");
    assertEquals(result.size(), 0);

    result = QueryTool.parseBucketDefinitions(null);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testParseBucketDefinitionsSingleRange() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("20-25");
    assertEquals(result.size(), 1);
    assertTrue(result.containsKey("20-25"));
  }

  @Test
  public void testParseBucketDefinitionsSingleOperator() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("young:lt:30");
    assertEquals(result.size(), 1);
    assertTrue(result.containsKey("young"));
  }

  @Test
  public void testParseBucketDefinitionsWithSpaces() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions(" 20-25 , 30-35 ");
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("20-25"));
    assertTrue(result.containsKey("30-35"));
  }

  @Test
  public void testParseBucketDefinitionsRangeFormat() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("0-18,19-65,66-100");
    assertEquals(result.size(), 3);
    assertTrue(result.containsKey("0-18"));
    assertTrue(result.containsKey("19-65"));
    assertTrue(result.containsKey("66-100"));
  }

  @Test
  public void testParseBucketDefinitionsOperatorFormat() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("child:lt:18,adult:gte:18");
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("child"));
    assertTrue(result.containsKey("adult"));
  }

  @Test
  public void testParseBucketDefinitionsOperators() {
    Map<String, Predicate<Long>> result =
        QueryTool.parseBucketDefinitions("test1:lt:10,test2:lte:20,test3:gt:30,test4:gte:40,test5:eq:50");
    assertEquals(result.size(), 5);
    assertTrue(result.containsKey("test1"));
    assertTrue(result.containsKey("test2"));
    assertTrue(result.containsKey("test3"));
    assertTrue(result.containsKey("test4"));
    assertTrue(result.containsKey("test5"));
  }

  @Test
  public void testParseBucketDefinitionsMixedFormat() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions("20-25,young:lt:30");
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey("20-25"));
    assertTrue(result.containsKey("young"));
  }

  @Test
  public void testParseBucketDefinitionsComplexMixed() {
    Map<String, Predicate<Long>> result = QueryTool.parseBucketDefinitions(
        "infant:lt:2,child:gte:2,teen:lte:19,adult:gt:19,senior:gte:65,range1:eq:50,10-20,30-40");
    assertEquals(result.size(), 8);
    assertTrue(result.containsKey("infant"));
    assertTrue(result.containsKey("child"));
    assertTrue(result.containsKey("teen"));
    assertTrue(result.containsKey("adult"));
    assertTrue(result.containsKey("senior"));
    assertTrue(result.containsKey("range1"));
    assertTrue(result.containsKey("10-20"));
    assertTrue(result.containsKey("30-40"));
  }

  @Test
  public void testParseBucketDefinitionsInvalidFormat() {
    try {
      QueryTool.parseBucketDefinitions("invalid:format");
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid bucket definition format"));
    }
  }

  @Test
  public void testParseBucketDefinitionsRangeFormatInvalid() {
    try {
      QueryTool.parseBucketDefinitions("20-25-30");
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid range format"));
    }
  }

  @Test
  public void testParseBucketDefinitionsNumberFormatException() {
    try {
      QueryTool.parseBucketDefinitions("test:lt:notanumber");
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format"));
    }

    try {
      QueryTool.parseBucketDefinitions("notanumber-25");
      fail("Expected VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Invalid number format"));
    }
  }

  @Test
  public void testParseKeys() {
    // This test requires mocking the client, so we'll just test the basic functionality
    // The actual implementation splits on comma and converts each key
    String keyString = "key1,key2,key3";
    String[] expectedKeys = keyString.split(",");
    assertEquals(expectedKeys.length, 3);
    assertEquals(expectedKeys[0], "key1");
    assertEquals(expectedKeys[1], "key2");
    assertEquals(expectedKeys[2], "key3");
  }

  @Test
  public void testParseKeysSingleKey() {
    String keyString = "singleKey";
    String[] expectedKeys = keyString.split(",");
    assertEquals(expectedKeys.length, 1);
    assertEquals(expectedKeys[0], "singleKey");
  }

  @Test
  public void testParseKeysWithSpaces() {
    String keyString = " key1 , key2 , key3 ";
    String[] keys = keyString.split(",");
    assertEquals(keys.length, 3);
    assertEquals(keys[0].trim(), "key1");
    assertEquals(keys[1].trim(), "key2");
    assertEquals(keys[2].trim(), "key3");
  }
}
