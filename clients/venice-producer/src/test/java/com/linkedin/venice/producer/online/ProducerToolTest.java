package com.linkedin.venice.producer.online;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.schema.RouterBasedStoreSchemaFetcher;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.producer.DurableWrite;
import com.linkedin.venice.writer.update.UpdateBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ProducerToolTest {
  private static final String RECORD_SCHEMA_STR = "{" + "\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
      + "{\"name\": \"name\", \"type\": \"string\"}," + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"score\", \"type\": \"double\"}," + "{\"name\": \"active\", \"type\": \"boolean\"},"
      + "{\"name\": \"nullable_field\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "{\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},"
      + "{\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}" + "]}";

  private static final Schema RECORD_SCHEMA = new Schema.Parser().parse(RECORD_SCHEMA_STR);
  private static final Schema KEY_SCHEMA = Schema.create(Schema.Type.STRING);

  private OnlineVeniceProducer mockProducer;
  private RouterBasedStoreSchemaFetcher mockSchemaFetcher;
  private CompletableFuture<DurableWrite> completedFuture;

  @BeforeMethod
  public void setUp() {
    mockProducer = mock(OnlineVeniceProducer.class);
    mockSchemaFetcher = mock(RouterBasedStoreSchemaFetcher.class);
    completedFuture = CompletableFuture.completedFuture(mock(DurableWrite.class));

    when(mockSchemaFetcher.getKeySchema()).thenReturn(KEY_SCHEMA);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(RECORD_SCHEMA);

    Map<Integer, Schema> schemaMap = new HashMap<>();
    schemaMap.put(1, RECORD_SCHEMA);
    when(mockSchemaFetcher.getAllValueSchemasWithId()).thenReturn(schemaMap);
  }

  // ==================== writeFromFile tests ====================

  @Test
  public void testWriteFromFile_putOperation() throws Exception {
    when(mockProducer.asyncPut(any(), any())).thenReturn(completedFuture);

    File tempFile = createTempJsonlFile(
        "{\"key\": \"key1\", \"value\": {\"name\": \"Alice\", \"age\": 30, \"score\": 1.0, \"active\": true, "
            + "\"nullable_field\": null, \"tags\": [], \"metadata\": {}}}");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncPut(eq("key1"), any());
    verify(mockProducer, never()).asyncDelete(any());
    verify(mockProducer, never()).asyncUpdate(any(), any());
  }

  @Test
  public void testWriteFromFile_deleteOperation() throws Exception {
    when(mockProducer.asyncDelete(any())).thenReturn(completedFuture);

    File tempFile = createTempJsonlFile("{\"key\": \"key1\", \"value\": \"null\", \"operation\": \"delete\"}");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncDelete(eq("key1"));
    verify(mockProducer, never()).asyncPut(any(), any());
  }

  @Test
  public void testWriteFromFile_updateOperation() throws Exception {
    when(mockProducer.asyncUpdate(any(), any())).thenReturn(completedFuture);

    File tempFile =
        createTempJsonlFile("{\"key\": \"key1\", \"value\": {\"name\": \"Bob\"}, \"operation\": \"update\"}");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncUpdate(eq("key1"), any());
    verify(mockProducer, never()).asyncPut(any(), any());
    verify(mockProducer, never()).asyncDelete(any());
  }

  @Test
  public void testWriteFromFile_mixedOperations() throws Exception {
    when(mockProducer.asyncPut(any(), any())).thenReturn(completedFuture);
    when(mockProducer.asyncDelete(any())).thenReturn(completedFuture);
    when(mockProducer.asyncUpdate(any(), any())).thenReturn(completedFuture);

    File tempFile = createTempJsonlFile(
        "{\"key\": \"k1\", \"value\": {\"name\": \"Alice\", \"age\": 30, \"score\": 1.0, \"active\": true, "
            + "\"nullable_field\": null, \"tags\": [], \"metadata\": {}}}",
        "{\"key\": \"k2\", \"value\": \"null\", \"operation\": \"delete\"}",
        "{\"key\": \"k3\", \"value\": {\"name\": \"Bob\"}, \"operation\": \"update\"}");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncPut(eq("k1"), any());
    verify(mockProducer, times(1)).asyncDelete(eq("k2"));
    verify(mockProducer, times(1)).asyncUpdate(eq("k3"), any());
  }

  @Test
  public void testWriteFromFile_skipsEmptyLines() throws Exception {
    when(mockProducer.asyncPut(any(), any())).thenReturn(completedFuture);

    File tempFile = createTempJsonlFile(
        "",
        "{\"key\": \"k1\", \"value\": {\"name\": \"Alice\", \"age\": 30, \"score\": 1.0, \"active\": true, "
            + "\"nullable_field\": null, \"tags\": [], \"metadata\": {}}}",
        "   ",
        "");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncPut(eq("k1"), any());
  }

  @Test
  public void testWriteFromFile_missingKeyThrows() throws Exception {
    File tempFile = createTempJsonlFile("{\"value\": \"something\"}");

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher));
    assertTrue(e.getMessage().contains("Line 1: Missing required field 'key'"));
  }

  @Test
  public void testWriteFromFile_missingValueThrows() throws Exception {
    File tempFile = createTempJsonlFile("{\"key\": \"k1\"}");

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher));
    assertTrue(e.getMessage().contains("Line 1: Missing required field 'value'"));
  }

  @Test
  public void testWriteFromFile_invalidJsonThrows() throws Exception {
    File tempFile = createTempJsonlFile("not valid json");

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher));
    assertTrue(e.getMessage().contains("Line 1: Failed to parse JSON"));
  }

  @Test
  public void testWriteFromFile_unknownOperationThrows() throws Exception {
    File tempFile = createTempJsonlFile("{\"key\": \"k1\", \"value\": \"v\", \"operation\": \"upsert\"}");

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher));
    assertTrue(e.getMessage().contains("Unknown operation 'upsert'"));
  }

  @Test
  public void testWriteFromFile_defaultOperationIsPut() throws Exception {
    when(mockProducer.asyncPut(any(), any())).thenReturn(completedFuture);

    // No "operation" field — should default to put
    File tempFile = createTempJsonlFile(
        "{\"key\": \"k1\", \"value\": {\"name\": \"Alice\", \"age\": 30, \"score\": 1.0, \"active\": true, "
            + "\"nullable_field\": null, \"tags\": [], \"metadata\": {}}}");

    ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher);

    verify(mockProducer, times(1)).asyncPut(eq("k1"), any());
  }

  @Test
  public void testWriteFromFile_stopsOnFirstError() throws Exception {
    when(mockProducer.asyncPut(any(), any())).thenReturn(completedFuture);

    // First line valid, second line missing value
    File tempFile = createTempJsonlFile(
        "{\"key\": \"k1\", \"value\": {\"name\": \"Alice\", \"age\": 30, \"score\": 1.0, \"active\": true, "
            + "\"nullable_field\": null, \"tags\": [], \"metadata\": {}}}",
        "{\"key\": \"k2\"}");

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.writeFromFile(tempFile.getAbsolutePath(), mockProducer, mockSchemaFetcher));
    assertTrue(e.getMessage().contains("Line 2: Missing required field 'value'"));

    // First record should have been written
    verify(mockProducer, times(1)).asyncPut(eq("k1"), any());
  }

  // ==================== buildUpdateFunctionWithSchema tests ====================

  @Test
  public void testBuildUpdateFunction_nullFieldValue() {
    String valueJson = "{\"nullable_field\": null}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setNewFieldValue("nullable_field", null);
  }

  @Test
  public void testBuildUpdateFunction_mixedNullAndNonNull() {
    String valueJson = "{\"name\": \"Bob\", \"nullable_field\": null, \"age\": 25}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setNewFieldValue("name", "Bob");
    verify(mockBuilder).setNewFieldValue("nullable_field", null);
    verify(mockBuilder).setNewFieldValue("age", 25);
  }

  @Test
  public void testBuildUpdateFunction_addToList() {
    String valueJson = "{\"tags\": {\"$addToList\": [\"tag1\", \"tag2\"]}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setElementsToAddToListField("tags", java.util.Arrays.asList("tag1", "tag2"));
  }

  @Test
  public void testBuildUpdateFunction_removeFromList() {
    String valueJson = "{\"tags\": {\"$removeFromList\": [\"old\"]}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setElementsToRemoveFromListField("tags", Collections.singletonList("old"));
  }

  @Test
  public void testBuildUpdateFunction_addToMap() {
    String valueJson = "{\"metadata\": {\"$addToMap\": {\"key1\": \"val1\"}}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setEntriesToAddToMapField("metadata", Collections.singletonMap("key1", "val1"));
  }

  @Test
  public void testBuildUpdateFunction_removeFromMap() {
    String valueJson = "{\"metadata\": {\"$removeFromMap\": [\"key1\"]}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setKeysToRemoveFromMapField("metadata", Collections.singletonList("key1"));
  }

  @Test
  public void testBuildUpdateFunction_nonRecordSchemaThrows() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("{\"field\": \"value\"}", stringSchema));
    assertTrue(e.getMessage().contains("Partial update requires a RECORD value schema"));
  }

  @Test
  public void testBuildUpdateFunction_unknownFieldThrows() {
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("{\"nonexistent\": \"value\"}", RECORD_SCHEMA));
    assertTrue(e.getMessage().contains("does not exist in the value schema"));
  }

  @Test
  public void testBuildUpdateFunction_emptyObjectThrows() {
    VeniceException e =
        expectThrows(VeniceException.class, () -> ProducerTool.buildUpdateFunctionWithSchema("{}", RECORD_SCHEMA));
    assertTrue(e.getMessage().contains("No update operations specified"));
  }

  @Test
  public void testBuildUpdateFunction_addToListOnNonArrayFieldThrows() {
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("{\"name\": {\"$addToList\": [\"val\"]}}", RECORD_SCHEMA));
    assertTrue(e.getMessage().contains("requires an ARRAY field"));
  }

  @Test
  public void testBuildUpdateFunction_addToMapOnNonMapFieldThrows() {
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("{\"name\": {\"$addToMap\": {\"k\": \"v\"}}}", RECORD_SCHEMA));
    assertTrue(e.getMessage().contains("requires a MAP field"));
  }

  // ==================== adaptDataToSchema tests ====================

  @Test
  public void testAdaptDataToSchema_primitiveTypes() {
    assertEquals(ProducerTool.adaptDataToSchema("42", Schema.create(Schema.Type.INT)), 42);
    assertEquals(ProducerTool.adaptDataToSchema("100", Schema.create(Schema.Type.LONG)), 100L);
    assertEquals(ProducerTool.adaptDataToSchema("1.25", Schema.create(Schema.Type.FLOAT)), 1.25f);
    assertEquals(ProducerTool.adaptDataToSchema("2.718", Schema.create(Schema.Type.DOUBLE)), 2.718);
    assertEquals(ProducerTool.adaptDataToSchema("true", Schema.create(Schema.Type.BOOLEAN)), true);
    assertEquals(ProducerTool.adaptDataToSchema("hello", Schema.create(Schema.Type.STRING)), "hello");
  }

  // ==================== jsonNodeToString tests ====================

  @Test
  public void testJsonNodeToString_stringSchemaUsesAsText() throws Exception {
    com.fasterxml.jackson.databind.JsonNode node =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree("\"hello\"");
    String result = ProducerTool.jsonNodeToString(node, Schema.create(Schema.Type.STRING));
    // asText() strips quotes: "hello" -> hello
    assertEquals(result, "hello");
  }

  @Test
  public void testJsonNodeToString_enumSchemaPreservesQuotes() throws Exception {
    Schema enumSchema = Schema.createEnum("Color", null, null, java.util.Arrays.asList("RED", "GREEN", "BLUE"));
    com.fasterxml.jackson.databind.JsonNode node =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree("\"RED\"");
    String result = ProducerTool.jsonNodeToString(node, enumSchema);
    // toString() preserves quotes for JSON decoder: "RED" -> "RED"
    assertEquals(result, "\"RED\"");
  }

  @Test
  public void testJsonNodeToString_numericNodeAlwaysUsesToString() throws Exception {
    com.fasterxml.jackson.databind.JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree("42");
    String result = ProducerTool.jsonNodeToString(node, Schema.create(Schema.Type.INT));
    assertEquals(result, "42");
  }

  @Test
  public void testAdaptDataToSchema_enumWithQuotedString() {
    Schema enumSchema = Schema.createEnum("Color", null, null, java.util.Arrays.asList("RED", "GREEN", "BLUE"));
    // Avro JSON decoder expects quoted enum value
    Object result = ProducerTool.adaptDataToSchema("\"RED\"", enumSchema);
    assertNotNull(result);
    assertEquals(result.toString(), "RED");
    assertTrue(result instanceof GenericData.EnumSymbol);
  }

  @Test
  public void testBuildUpdateFunction_enumField() {
    String schemaWithEnum = "{" + "\"type\": \"record\", \"name\": \"TestWithEnum\", \"fields\": ["
        + "{\"name\": \"color\", \"type\": {\"type\": \"enum\", \"name\": \"Color\", "
        + "\"symbols\": [\"RED\", \"GREEN\", \"BLUE\"]}}" + "]}";
    Schema schema = new Schema.Parser().parse(schemaWithEnum);

    // JSON input has "RED" as a textual node — jsonNodeToString should preserve quotes for enum
    String valueJson = "{\"color\": \"RED\"}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, schema);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    // The adapted value should be a GenericData.EnumSymbol
    verify(mockBuilder).setNewFieldValue(eq("color"), any(GenericData.EnumSymbol.class));
  }

  // ==================== helpers ====================

  private File createTempJsonlFile(String... lines) throws Exception {
    File tempFile = File.createTempFile("producer-tool-test", ".jsonl");
    tempFile.deleteOnExit();
    try (PrintWriter writer = new PrintWriter(new FileWriter(tempFile))) {
      for (String line: lines) {
        writer.println(line);
      }
    }
    return tempFile;
  }
}
