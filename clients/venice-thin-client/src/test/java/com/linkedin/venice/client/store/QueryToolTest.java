package com.linkedin.venice.client.store;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class QueryToolTest {
  // --- removeQuotes tests ---
  @Test
  public void testRemoveQuotesBothQuotes() {
    assertEquals(QueryTool.removeQuotes("\"hello\""), "hello");
  }

  @Test
  public void testRemoveQuotesLeadingOnly() {
    assertEquals(QueryTool.removeQuotes("\"hello"), "hello");
  }

  @Test
  public void testRemoveQuotesTrailingOnly() {
    assertEquals(QueryTool.removeQuotes("hello\""), "hello");
  }

  @Test
  public void testRemoveQuotesNone() {
    assertEquals(QueryTool.removeQuotes("hello"), "hello");
  }

  @Test
  public void testRemoveQuotesEmpty() {
    assertEquals(QueryTool.removeQuotes(""), "");
  }

  @Test
  public void testRemoveQuotesOnlyQuotes() {
    // "\"\"" → remove leading → "\"" → remove trailing → ""
    assertEquals(QueryTool.removeQuotes("\"\""), "");
  }

  @Test
  public void testRemoveQuotesEmbeddedQuotes() {
    assertEquals(QueryTool.removeQuotes("he\"llo"), "he\"llo");
  }

  @Test
  public void testRemoveQuotesSingleDoubleQuote() {
    // A single double-quote character — removing surrounding quotes yields an empty string
    assertEquals(QueryTool.removeQuotes("\""), "");
  }

  // --- convertKey tests ---

  @Test
  public void testConvertKeyInt() {
    Object result = QueryTool.convertKey("42", Schema.create(Schema.Type.INT));
    assertEquals(result, 42);
  }

  @Test
  public void testConvertKeyLong() {
    Object result = QueryTool.convertKey("9999999999", Schema.create(Schema.Type.LONG));
    assertEquals(result, 9999999999L);
  }

  @Test
  public void testConvertKeyFloat() {
    Object result = QueryTool.convertKey("1.5", Schema.create(Schema.Type.FLOAT));
    assertEquals(result, 1.5f);
  }

  @Test
  public void testConvertKeyDouble() {
    Object result = QueryTool.convertKey("123.456", Schema.create(Schema.Type.DOUBLE));
    assertEquals(result, 123.456);
  }

  @Test
  public void testConvertKeyBoolean() {
    Object result = QueryTool.convertKey("true", Schema.create(Schema.Type.BOOLEAN));
    assertEquals(result, true);
  }

  @Test
  public void testConvertKeyString() {
    Object result = QueryTool.convertKey("hello", Schema.create(Schema.Type.STRING));
    assertEquals(result, "hello");
  }

  @Test
  public void testConvertKeyRecord() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"memberId\",\"type\":\"long\"},{\"name\":\"useCaseName\",\"type\":\"string\"}]}";
    Schema schema = new Schema.Parser().parse(schemaJson);
    String keyJson = "{\"memberId\": 220896326, \"useCaseName\": \"nba_digest_email\"}";
    Object result = QueryTool.convertKey(keyJson, schema);
    GenericRecord record = (GenericRecord) result;
    assertEquals(record.get("memberId"), 220896326L);
    assertEquals(record.get("useCaseName").toString(), "nba_digest_email");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testConvertKeyInvalidJson() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
    Schema schema = new Schema.Parser().parse(schemaJson);
    QueryTool.convertKey("not valid json", schema);
  }

  // --- parseKeys tests (the core fix) ---

  @Test
  public void testParseKeysSinglePrimitive() {
    List<String> result = QueryTool.parseKeys("42");
    assertEquals(result, Collections.singletonList("42"));
  }

  @Test
  public void testParseKeysMultiplePrimitives() {
    List<String> result = QueryTool.parseKeys("1,2,3");
    assertEquals(result, Arrays.asList("1", "2", "3"));
  }

  @Test
  public void testParseKeysSingleComplexJsonKey() {
    // THE BUG: a single JSON key with commas must NOT be split
    String jsonKey = "{\"memberId\":220896326,\"useCaseName\":\"nba_digest_email\"}";
    List<String> result = QueryTool.parseKeys(jsonKey);
    assertEquals(result.size(), 1, "Single JSON key should not be split on internal commas");
    assertEquals(result.get(0), jsonKey);
  }

  @Test
  public void testParseKeysMultipleComplexJsonKeys() {
    String input = "{\"memberId\":1,\"name\":\"a\"},{\"memberId\":2,\"name\":\"b\"}";
    List<String> result = QueryTool.parseKeys(input);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "{\"memberId\":1,\"name\":\"a\"}");
    assertEquals(result.get(1), "{\"memberId\":2,\"name\":\"b\"}");
  }

  @Test
  public void testParseKeysNestedBraces() {
    // Keys with nested objects
    String input = "{\"a\":{\"b\":1},\"c\":2},{\"a\":{\"b\":3},\"c\":4}";
    List<String> result = QueryTool.parseKeys(input);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "{\"a\":{\"b\":1},\"c\":2}");
    assertEquals(result.get(1), "{\"a\":{\"b\":3},\"c\":4}");
  }

  @Test
  public void testParseKeysWithArrayBrackets() {
    // Keys containing arrays — brackets must not cause incorrect splitting
    String input = "{\"ids\":[1,2,3],\"name\":\"a\"},{\"ids\":[4,5],\"name\":\"b\"}";
    List<String> result = QueryTool.parseKeys(input);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0), "{\"ids\":[1,2,3],\"name\":\"a\"}");
    assertEquals(result.get(1), "{\"ids\":[4,5],\"name\":\"b\"}");
  }

  @Test
  public void testParseKeysWhitespace() {
    List<String> result = QueryTool.parseKeys("  1 , 2 , 3  ");
    assertEquals(result, Arrays.asList("1", "2", "3"));
  }

  @Test
  public void testParseKeysEmptyInput() {
    assertEquals(QueryTool.parseKeys(""), Collections.emptyList());
    assertEquals(QueryTool.parseKeys("  "), Collections.emptyList());
    assertEquals(QueryTool.parseKeys(null), Collections.emptyList());
  }

  // --- End-to-end: parseKeys → convertKey pipeline ---

  @Test
  public void testParseKeysThenConvertComplexKey() {
    // This is the exact regression scenario from PR #1920
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"memberId\",\"type\":\"long\"},{\"name\":\"useCaseName\",\"type\":\"string\"}]}";
    Schema schema = new Schema.Parser().parse(schemaJson);

    // Single complex key passed to parseKeys
    String input = "{\"memberId\":220896326,\"useCaseName\":\"nba_digest_email\"}";
    List<String> keys = QueryTool.parseKeys(input);
    assertEquals(keys.size(), 1, "Must not split JSON key on internal commas");

    // Then convert it — should succeed, not throw
    Object result = QueryTool.convertKey(keys.get(0), schema);
    GenericRecord record = (GenericRecord) result;
    assertEquals(record.get("memberId"), 220896326L);
    assertEquals(record.get("useCaseName").toString(), "nba_digest_email");
  }

  @Test
  public void testParseKeysThenConvertMultipleComplexKeys() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"memberId\",\"type\":\"long\"},{\"name\":\"useCaseName\",\"type\":\"string\"}]}";
    Schema schema = new Schema.Parser().parse(schemaJson);

    String input =
        "{\"memberId\":220896326,\"useCaseName\":\"nba_digest_email\"},{\"memberId\":12345,\"useCaseName\":\"test\"}";
    List<String> keys = QueryTool.parseKeys(input);
    assertEquals(keys.size(), 2);

    GenericRecord r1 = (GenericRecord) QueryTool.convertKey(keys.get(0), schema);
    assertEquals(r1.get("memberId"), 220896326L);

    GenericRecord r2 = (GenericRecord) QueryTool.convertKey(keys.get(1), schema);
    assertEquals(r2.get("memberId"), 12345L);
    assertEquals(r2.get("useCaseName").toString(), "test");
  }

  // --- parseFlags tests ---

  @Test
  public void testParseFlagsKeyValue() {
    String[] args = { "store", "key", "url", "false", "", "--fields=f1,f2", "--topK=5" };
    Map<String, String> flags = QueryTool.parseFlags(args, 5);
    assertEquals(flags.get("fields"), "f1,f2");
    assertEquals(flags.get("topK"), "5");
  }

  @Test
  public void testParseFlagsBooleanFlag() {
    String[] args = { "store", "key", "url", "false", "", "--countByValue" };
    Map<String, String> flags = QueryTool.parseFlags(args, 5);
    assertEquals(flags.get("countByValue"), "true");
  }

  @Test
  public void testParseFlagsNoFlags() {
    String[] args = { "store", "key", "url", "false", "" };
    Map<String, String> flags = QueryTool.parseFlags(args, 5);
    assertEquals(flags.size(), 0);
  }

  @Test
  public void testParseFlagsSkipsNonDashArgs() {
    String[] args = { "store", "key", "url", "false", "", "notAFlag", "--real=yes" };
    Map<String, String> flags = QueryTool.parseFlags(args, 5);
    assertEquals(flags.size(), 1);
    assertEquals(flags.get("real"), "yes");
  }
}
