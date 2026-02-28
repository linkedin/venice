package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;

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
  public void testRemoveQuotesSingleQuote() {
    // A single quote character — starts with " and after removing leading, result is empty, no trailing to strip
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
    Object result = QueryTool.convertKey("3.14", Schema.create(Schema.Type.FLOAT));
    assertEquals(result, 3.14f);
  }

  @Test
  public void testConvertKeyDouble() {
    Object result = QueryTool.convertKey("2.718281828", Schema.create(Schema.Type.DOUBLE));
    assertEquals(result, 2.718281828);
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
