package com.linkedin.venice.schema;

import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSupersetSchemaUtils {

  @Test
  public void testGenerateSupersetSchemaFromValueSchemasWithTwoSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}"
        + " ]"
        + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}"
        + " ]"
        + "}";

    SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);

    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2)
    );

    final Schema expectedSupersetSchema = AvroSchemaUtils.generateSuperSetSchema(schemaEntry1.getSchema(), schemaEntry2.getSchema());
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry2, schemaEntry1) // Order should not matter.
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    // Test the case where generated superset schema entry should have schema ID 1 instead of 2.
    schemaEntry1 = new SchemaEntry(2, schemaStr1);
    schemaEntry2 = new SchemaEntry(1, schemaStr2);

    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry2, schemaEntry1) // Order should not matter.
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 1);
  }

  @Test
  public void testGenerateSupersetSchemaFromThreeValueSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}"
        + " ]"
        + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}" // New field compared to schemaStr1
        + " ]"
        + "}";

    String schemaStr3 = "{\"type\":\"record\",\"name\":\"KeyRecord\","
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}" // Another new field compared to schemaStr1
        + " ]"
        + "}";

    String schemaStr4 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," // This schema contains all fields.
        + "\"fields\":"
        + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}"
        + " ]"
        + "}";

    final SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    final SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);
    final SchemaEntry schemaEntry3 = new SchemaEntry(3, schemaStr3);
    final SchemaEntry schemaEntry4 = new SchemaEntry(4, schemaStr4);
    final Schema expectedSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr4);

    // Case 1: generate a superset new schema entry with a new ID.
    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3)
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 2: generate a superset schema entry that is the same as schemaEntry4.
    supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
        Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry4)
    );
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 3: any schema + schemaEntry4 can generate a schema entry that is the same as schemaEntry4.
    for (SchemaEntry schemaEntry : Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3)) {
      supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
          Arrays.asList(schemaEntry, schemaEntry4)
      );
      Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);

      supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(
          Arrays.asList(schemaEntry4, schemaEntry) // Order should not matter.
      );
      Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);
    }
  }
}
