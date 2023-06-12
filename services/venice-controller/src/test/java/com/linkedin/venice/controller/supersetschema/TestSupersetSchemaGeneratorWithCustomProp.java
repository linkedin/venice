package com.linkedin.venice.controller.supersetschema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestWriteUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class TestSupersetSchemaGeneratorWithCustomProp {
  private static final String CUSTOM_PROP = "custom_prop";

  private Schema schemaV1 =
      AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("superset_schema_test/v1.avsc"));
  private Schema schemaV1WithoutCustomProp = AvroCompatibilityHelper
      .parse(TestWriteUtils.loadFileAsString("superset_schema_test/v1_without_custom_prop.avsc"));
  private Schema schemaV2 =
      AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("superset_schema_test/v2.avsc"));
  private Schema schemaV3 =
      AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("superset_schema_test/v3.avsc"));
  private Schema schemaV4 = AvroCompatibilityHelper
      .parse(TestWriteUtils.loadFileAsString("superset_schema_test/v4_without_custom_prop.avsc"));

  private SupersetSchemaGenerator generator;

  public TestSupersetSchemaGeneratorWithCustomProp() throws IOException {
    this.generator = new SupersetSchemaGeneratorWithCustomProp(CUSTOM_PROP);
  }

  @Test
  public void testGenerateSupersetSchemaFromSchemas() throws IOException {

    // Two partially-overlapped schemas
    Collection<SchemaEntry> schemaEntryCollection1 =
        Arrays.asList(new SchemaEntry(1, schemaV1), new SchemaEntry(2, schemaV2));
    SchemaEntry supersetSchemaEntry1 = generator.generateSupersetSchemaFromSchemas(schemaEntryCollection1);
    assertEquals(supersetSchemaEntry1.getId(), 3);
    Schema supersetSchema1 = supersetSchemaEntry1.getSchema();
    assertEquals(supersetSchema1.getProp(CUSTOM_PROP), "custom_prop_value_for_v2");
    assertNotNull(supersetSchema1.getField("f0"));
    assertNotNull(supersetSchema1.getField("f1"));
    assertNotNull(supersetSchema1.getField("f2"));

    // v3 is a superset of v1 and v2
    Collection<SchemaEntry> schemaEntryCollection2 =
        Arrays.asList(new SchemaEntry(1, schemaV1), new SchemaEntry(2, schemaV2), new SchemaEntry(3, schemaV3));
    SchemaEntry supersetSchemaEntry2 = generator.generateSupersetSchemaFromSchemas(schemaEntryCollection2);
    assertEquals(supersetSchemaEntry2.getId(), 3);
    assertEquals(supersetSchemaEntry2.getSchema(), schemaV3);
    Schema supersetSchema2 = supersetSchemaEntry2.getSchema();
    assertEquals(supersetSchema2.getProp(CUSTOM_PROP), "custom_prop_value_for_v3");

    // v4 doesn't contain custom_prop
    Collection<SchemaEntry> schemaEntryCollection3 = Arrays.asList(
        new SchemaEntry(1, schemaV1),
        new SchemaEntry(2, schemaV2),
        new SchemaEntry(3, schemaV3),
        new SchemaEntry(4, schemaV4));
    SchemaEntry supersetSchemaEntry3 = generator.generateSupersetSchemaFromSchemas(schemaEntryCollection3);
    assertEquals(supersetSchemaEntry3.getId(), 5);
    Schema supersetSchema3 = supersetSchemaEntry3.getSchema();
    assertNull(supersetSchema3.getProp(CUSTOM_PROP));
    assertNotNull(supersetSchema3.getField("f0"));
    assertNotNull(supersetSchema3.getField("f1"));
    assertNotNull(supersetSchema3.getField("f2"));
    assertNotNull(supersetSchema3.getField("f3"));
  }

  @Test
  public void testCompareSchema() {
    assertTrue(generator.compareSchema(schemaV1, schemaV1));

    // Test schemas with diff prop
    assertFalse(generator.compareSchema(schemaV1, schemaV1WithoutCustomProp));
    assertFalse(generator.compareSchema(schemaV1WithoutCustomProp, schemaV1));

    // Test schemas with different fields
    assertFalse(generator.compareSchema(schemaV1, schemaV2));

    // Two schemas without custom prop
    assertTrue(generator.compareSchema(schemaV1WithoutCustomProp, schemaV1WithoutCustomProp));
  }

  @Test
  public void testGenerateSupersetSchema() {
    // Two schemas with custom prop
    Schema supersetSchema1 = generator.generateSupersetSchema(schemaV1, schemaV2);
    assertEquals(supersetSchema1.getProp(CUSTOM_PROP), "custom_prop_value_for_v2");
    assertNotNull(supersetSchema1.getField("f0"));
    assertNotNull(supersetSchema1.getField("f1"));
    assertNotNull(supersetSchema1.getField("f2"));

    // New schema without custom prop
    Schema supersetSchema2 = generator.generateSupersetSchema(schemaV2, schemaV1WithoutCustomProp);
    assertNull(supersetSchema2.getProp(CUSTOM_PROP));
    assertNotNull(supersetSchema2.getField("f0"));
    assertNotNull(supersetSchema2.getField("f1"));
    assertNotNull(supersetSchema2.getField("f2"));
  }
}
