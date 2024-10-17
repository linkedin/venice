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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
  private Schema schemaV5 =
      AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("superset_schema_test/v5.avsc"));
  private Schema schemaV6 =
      AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("superset_schema_test/v6.avsc"));

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

    // v5 contains all fields in superset schema and a custom prop
    Collection<SchemaEntry> schemaEntryCollection4 = Arrays.asList(
        new SchemaEntry(1, schemaV1),
        new SchemaEntry(2, schemaV2),
        new SchemaEntry(3, schemaV3),
        new SchemaEntry(4, schemaV4),
        new SchemaEntry(5, schemaV5));
    SchemaEntry supersetSchemaEntry4 = generator.generateSupersetSchemaFromSchemas(schemaEntryCollection4);
    assertEquals(supersetSchemaEntry4.getId(), 5);
    Schema supersetSchema4 = supersetSchemaEntry4.getSchema();
    assertEquals(supersetSchema4.getProp(CUSTOM_PROP), "custom_prop_value_for_v5");
    assertNotNull(supersetSchema4.getField("f0"));
    assertNotNull(supersetSchema4.getField("f1"));
    assertNotNull(supersetSchema4.getField("f2"));
    assertNotNull(supersetSchema4.getField("f3"));

    // v6 contains a subset of fields, but with a different custom prop
    Collection<SchemaEntry> schemaEntryCollection5 = Arrays.asList(
        new SchemaEntry(1, schemaV1),
        new SchemaEntry(2, schemaV2),
        new SchemaEntry(3, schemaV3),
        new SchemaEntry(4, schemaV4),
        new SchemaEntry(5, schemaV5),
        new SchemaEntry(6, schemaV6));
    SchemaEntry supersetSchemaEntry5 = generator.generateSupersetSchemaFromSchemas(schemaEntryCollection5);
    assertEquals(supersetSchemaEntry5.getId(), 7);
    Schema supersetSchema5 = supersetSchemaEntry5.getSchema();
    assertEquals(supersetSchema5.getProp(CUSTOM_PROP), "custom_prop_value_for_v6");
    assertNotNull(supersetSchema5.getField("f0"));
    assertNotNull(supersetSchema5.getField("f1"));
    assertNotNull(supersetSchema5.getField("f2"));
    assertNotNull(supersetSchema5.getField("f3"));
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

  @Test
  public void testGenerateSupersetSchemaWithExistingCustomProp() {
    /**
     * Give a list of schemas and the 2nd schema will remove some optional field.
     */
    Schema schema1 = Schema.parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"int_field\", \"type\": \"int\", \"default\": 0},\n"
            + "    {\"name\": \"string_field\", \"type\": \"string\", \"default\": \"\"}\n" + "  ],\n"
            + "  \"custom_prop\": \"custom_prop1\"\n" + "}");
    Schema schema2 = Schema.parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"int_field\", \"type\": \"int\", \"default\": 0}\n" + "  ],\n"
            + "  \"custom_prop\": \"custom_prop2\"\n" + "}\n");

    SchemaEntry schemaEntry1 = new SchemaEntry(1, schema1);
    SchemaEntry schemaEntry2 = new SchemaEntry(2, schema2);
    List<SchemaEntry> schemaEntryList = new ArrayList<>();
    schemaEntryList.add(schemaEntry1);
    schemaEntryList.add(schemaEntry2);

    // Make sure there is no exception thrown
    generator.generateSupersetSchemaFromSchemas(schemaEntryList);
    generator.generateSupersetSchema(schema1, schema2);
  }
}
