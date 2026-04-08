package com.linkedin.venice.producer.online;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.writer.update.UpdateBuilder;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class ProducerToolTest {
  private static final String RECORD_SCHEMA_STR = "{" + "\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
      + "{\"name\": \"name\", \"type\": \"string\"}," + "{\"name\": \"age\", \"type\": \"int\"},"
      + "{\"name\": \"score\", \"type\": \"double\"}," + "{\"name\": \"active\", \"type\": \"boolean\"},"
      + "{\"name\": \"nullable_field\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "{\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},"
      + "{\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}" + "]}";

  private static final Schema RECORD_SCHEMA = new Schema.Parser().parse(RECORD_SCHEMA_STR);

  @Test
  public void testBuildUpdateFunction_plainFieldReplacement() {
    String valueJson = "{\"name\": \"Alice\", \"age\": 30}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setNewFieldValue("name", "Alice");
    verify(mockBuilder).setNewFieldValue("age", 30);
  }

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

    verify(mockBuilder).setElementsToRemoveFromListField("tags", java.util.Collections.singletonList("old"));
  }

  @Test
  public void testBuildUpdateFunction_addToMap() {
    String valueJson = "{\"metadata\": {\"$addToMap\": {\"key1\": \"val1\"}}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setEntriesToAddToMapField("metadata", java.util.Collections.singletonMap("key1", "val1"));
  }

  @Test
  public void testBuildUpdateFunction_removeFromMap() {
    String valueJson = "{\"metadata\": {\"$removeFromMap\": [\"key1\"]}}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setKeysToRemoveFromMapField("metadata", java.util.Collections.singletonList("key1"));
  }

  @Test
  public void testBuildUpdateFunction_nonRecordSchemaThrows() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("{\"field\": \"value\"}", stringSchema));
    assert e.getMessage().contains("Partial update requires a RECORD value schema");
  }

  @Test
  public void testBuildUpdateFunction_unknownFieldThrows() {
    String valueJson = "{\"nonexistent\": \"value\"}";
    VeniceException e =
        expectThrows(VeniceException.class, () -> ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA));
    assert e.getMessage().contains("does not exist in the value schema");
  }

  @Test
  public void testBuildUpdateFunction_invalidJsonThrows() {
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> ProducerTool.buildUpdateFunctionWithSchema("not json", RECORD_SCHEMA));
    assert e.getMessage().contains("Failed to parse update value as JSON");
  }

  @Test
  public void testBuildUpdateFunction_emptyObjectThrows() {
    VeniceException e =
        expectThrows(VeniceException.class, () -> ProducerTool.buildUpdateFunctionWithSchema("{}", RECORD_SCHEMA));
    assert e.getMessage().contains("No update operations specified");
  }

  @Test
  public void testBuildUpdateFunction_addToListOnNonArrayFieldThrows() {
    String valueJson = "{\"name\": {\"$addToList\": [\"val\"]}}";
    VeniceException e =
        expectThrows(VeniceException.class, () -> ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA));
    assert e.getMessage().contains("requires an ARRAY field");
  }

  @Test
  public void testBuildUpdateFunction_addToMapOnNonMapFieldThrows() {
    String valueJson = "{\"name\": {\"$addToMap\": {\"k\": \"v\"}}}";
    VeniceException e =
        expectThrows(VeniceException.class, () -> ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA));
    assert e.getMessage().contains("requires a MAP field");
  }

  @Test
  public void testAdaptDataToSchema_primitiveTypes() {
    assertEquals(ProducerTool.adaptDataToSchema("42", Schema.create(Schema.Type.INT)), 42);
    assertEquals(ProducerTool.adaptDataToSchema("100", Schema.create(Schema.Type.LONG)), 100L);
    assertEquals(ProducerTool.adaptDataToSchema("3.14", Schema.create(Schema.Type.FLOAT)), 3.14f);
    assertEquals(ProducerTool.adaptDataToSchema("2.718", Schema.create(Schema.Type.DOUBLE)), 2.718);
    assertEquals(ProducerTool.adaptDataToSchema("true", Schema.create(Schema.Type.BOOLEAN)), true);
    assertEquals(ProducerTool.adaptDataToSchema("hello", Schema.create(Schema.Type.STRING)), "hello");
  }

  @Test
  public void testBuildUpdateFunction_booleanAndDoubleFields() {
    String valueJson = "{\"active\": true, \"score\": 99.5}";
    Consumer<UpdateBuilder> updateFn = ProducerTool.buildUpdateFunctionWithSchema(valueJson, RECORD_SCHEMA);

    UpdateBuilder mockBuilder = mock(UpdateBuilder.class);
    updateFn.accept(mockBuilder);

    verify(mockBuilder).setNewFieldValue("active", true);
    verify(mockBuilder).setNewFieldValue("score", 99.5);
  }
}
