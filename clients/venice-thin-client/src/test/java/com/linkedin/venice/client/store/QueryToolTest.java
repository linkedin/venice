package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryToolTest {
  @SuppressWarnings("unchecked")
  private StatTrackingStoreClient<Object, Object> createMockClient(Schema keySchema, Map<Object, Object> batchResults) {
    AbstractAvroStoreClient<Object, Object> mockInnerClient = mock(AbstractAvroStoreClient.class);
    doReturn(keySchema).when(mockInnerClient).getKeySchema();

    StatTrackingStoreClient<Object, Object> mockStatClient = mock(StatTrackingStoreClient.class);
    doReturn(mockInnerClient).when(mockStatClient).getInnerStoreClient();

    CompletableFuture<Map<Object, Object>> future = CompletableFuture.completedFuture(batchResults);
    doReturn(future).when(mockStatClient).batchGet(any());

    return mockStatClient;
  }

  @Test
  public void testBatchQueryStoreForKeys() throws Exception {
    Map<Object, Object> batchResults = new LinkedHashMap<>();
    batchResults.put("key1", "value1");
    // key2 intentionally missing from results to test null value path

    StatTrackingStoreClient<Object, Object> mockClient =
        createMockClient(Schema.create(Schema.Type.STRING), batchResults);

    try (MockedStatic<ClientFactory> mockedFactory = mockStatic(ClientFactory.class)) {
      mockedFactory.when(() -> ClientFactory.getAndStartGenericAvroClient(any(ClientConfig.class)))
          .thenReturn(mockClient);

      Set<String> keys = new LinkedHashSet<>(Arrays.asList("key1", "key2"));
      Map<String, Map<String, String>> results =
          QueryTool.batchQueryStoreForKeys("testStore", keys, "http://localhost:1234", false, Optional.empty());

      Assert.assertEquals(results.size(), 2);
      Assert.assertEquals(results.get("key1").get("key"), "key1");
      Assert.assertEquals(results.get("key1").get("value"), "value1");
      Assert.assertEquals(results.get("key2").get("key"), "key2");
      Assert.assertEquals(results.get("key2").get("value"), "null");
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*SSL configuration is not valid.*")
  public void testBatchQueryStoreForKeysHttpsWithoutSsl() throws Exception {
    Set<String> keys = new LinkedHashSet<>(Arrays.asList("key1"));
    QueryTool.batchQueryStoreForKeys("testStore", keys, "https://localhost:1234", false, Optional.empty());
  }

  @Test
  public void testMainBatchMode() throws Exception {
    Map<Object, Object> batchResults = new LinkedHashMap<>();
    batchResults.put("key1", "value1");

    StatTrackingStoreClient<Object, Object> mockClient =
        createMockClient(Schema.create(Schema.Type.STRING), batchResults);

    try (MockedStatic<ClientFactory> mockedFactory = mockStatic(ClientFactory.class)) {
      mockedFactory.when(() -> ClientFactory.getAndStartGenericAvroClient(any(ClientConfig.class)))
          .thenReturn(mockClient);

      String[] args = { "--batch", "testStore", "http://localhost:1234", "false", "", "key1" };
      QueryTool.main(args);
    }
  }

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

  // --- parseBucketPredicates tests ---

  @SuppressWarnings("unchecked")
  private static void assertPredicate(
      String op,
      String valueStr,
      Class<?> expectedType,
      Object trueVal,
      Object falseVal) {
    Predicate p = QueryTool.createTypedPredicate(op, valueStr);
    assertTrue(expectedType.isInstance(p), "Expected " + expectedType.getSimpleName());
    assertTrue(p.evaluate(trueVal), op + " should accept " + trueVal);
    assertFalse(p.evaluate(falseVal), op + " should reject " + falseVal);
  }

  @Test
  public void testBucketPredicatesLong() {
    // Whole numbers default to LongPredicate — all 5 operators
    assertPredicate("eq", "100", LongPredicate.class, 100L, 99L);
    assertPredicate("gt", "100", LongPredicate.class, 101L, 100L);
    assertPredicate("gte", "100", LongPredicate.class, 100L, 99L);
    assertPredicate("lt", "100", LongPredicate.class, 99L, 100L);
    assertPredicate("lte", "100", LongPredicate.class, 100L, 101L);
    // Explicit 'L' suffix
    assertPredicate("gt", "100L", LongPredicate.class, 101L, 100L);
    // Value exceeding Integer.MAX_VALUE
    assertPredicate("gte", "3000000000", LongPredicate.class, 3000000000L, 2999999999L);
  }

  @Test
  public void testBucketPredicatesInt() {
    // Explicit 'i'/'I' suffix forces IntPredicate — all 5 operators
    assertPredicate("eq", "100i", IntPredicate.class, 100, 99);
    assertPredicate("gt", "100i", IntPredicate.class, 101, 100);
    assertPredicate("gte", "100I", IntPredicate.class, 100, 99);
    assertPredicate("lt", "100i", IntPredicate.class, 99, 100);
    assertPredicate("lte", "100i", IntPredicate.class, 100, 101);
  }

  @Test
  public void testBucketPredicatesDouble() {
    // Decimal values → DoublePredicate — all 5 operators
    assertPredicate("eq", "99.5", DoublePredicate.class, 99.5, 99.6);
    assertPredicate("gt", "99.5", DoublePredicate.class, 100.0, 99.5);
    assertPredicate("gte", "99.5", DoublePredicate.class, 99.5, 99.4);
    assertPredicate("lt", "99.5", DoublePredicate.class, 99.0, 99.5);
    assertPredicate("lte", "99.5", DoublePredicate.class, 99.5, 99.6);
    // Explicit 'd' suffix
    assertPredicate("lt", "99.5d", DoublePredicate.class, 99.0, 99.5);
  }

  @Test
  public void testBucketPredicatesStringEq() {
    Predicate p = QueryTool.createTypedPredicate("eq", "engineer");
    assertTrue(p.evaluate("engineer"));
    assertFalse(p.evaluate("manager"));
  }

  @Test
  public void testParseBucketPredicatesMultipleBuckets() {
    Map<String, Predicate> predicates = QueryTool.parseBucketPredicates("high:gt:100,low:lte:100");
    assertEquals(predicates.size(), 2);
    assertTrue(predicates.get("high").evaluate(101L));
    assertFalse(predicates.get("high").evaluate(100L));
    assertTrue(predicates.get("low").evaluate(100L));
    assertFalse(predicates.get("low").evaluate(101L));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesInvalidFormat() {
    QueryTool.parseBucketPredicates("missing_parts");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesUnknownOperator() {
    QueryTool.createTypedPredicate("foo", "123");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesStringUnsupportedOperator() {
    QueryTool.createTypedPredicate("gt", "hello");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesInvalidIntSuffix() {
    QueryTool.createTypedPredicate("gt", "abci");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesInvalidLongSuffix() {
    QueryTool.createTypedPredicate("gt", "abcL");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testBucketPredicatesInvalidDoubleSuffix() {
    QueryTool.createTypedPredicate("gt", "abcd");
  }
}
