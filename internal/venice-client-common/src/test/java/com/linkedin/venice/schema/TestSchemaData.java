package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchemaData {
  private static final Logger LOGGER = LogManager.getLogger(TestSchemaData.class);

  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(loadFileAsString("testSchemaData.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);

  @Test
  public void testAddKeySchema() {
    String keySchemaStr = "\"string\"";
    int id = 1;
    SchemaEntry entry = new SchemaEntry(id, keySchemaStr);
    SchemaData schemaData = new SchemaData("test_store", entry);

    SchemaEntry keySchema = schemaData.getKeySchema();
    Assert.assertEquals(keySchema.getId(), 1);
    Assert.assertEquals(keySchemaStr, keySchema.getSchema().toString());
  }

  @Test
  public void testAddValueSchema() {
    String valueSchemaStr1 = "\"long\"";
    String valueSchemaStr2 = "\"string\"";

    SchemaData schemaData = new SchemaData("test_store", null);
    schemaData.addValueSchema(new SchemaEntry(1, valueSchemaStr1));
    schemaData.addValueSchema(new SchemaEntry(2, valueSchemaStr2));

    Assert.assertEquals(schemaData.getSchemaID(new SchemaEntry(10, valueSchemaStr1)), 1);
    Assert.assertEquals(schemaData.getSchemaID(new SchemaEntry(10, valueSchemaStr2)), 2);
    Assert.assertEquals(schemaData.getMaxValueSchemaId(), 2);
    Assert.assertEquals(new SchemaEntry(10, valueSchemaStr1), schemaData.getValueSchema(1));
  }

  @Test
  public void testSchemaData() {
    SchemaEntry valueSchema = new SchemaEntry(1, VALUE_SCHEMA.toString());
    DerivedSchemaEntry derivedSchema = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    RmdSchemaEntry rmdSchema = new RmdSchemaEntry(1, 1, UPDATE_SCHEMA.toString());
    SchemaData schemaData = new SchemaData("testStore", null);

    Assert.assertEquals(schemaData.getDerivedSchemaId(derivedSchema.getSchemaStr()), GeneratedSchemaID.INVALID);

    schemaData.addValueSchema(valueSchema);
    schemaData.addDerivedSchema(derivedSchema);
    schemaData.addReplicationMetadataSchema(rmdSchema);
    Assert.assertEquals(schemaData.getDerivedSchemas(), Collections.singletonList(derivedSchema));
    Assert.assertEquals(schemaData.getReplicationMetadataSchemas(), Collections.singletonList(rmdSchema));
    Assert.assertEquals(schemaData.getDerivedSchemaId(derivedSchema.getSchemaStr()), new GeneratedSchemaID(1, 1));
  }

  private static String loadFileAsString(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOGGER.error(e);
      return null;
    }
  }
}
