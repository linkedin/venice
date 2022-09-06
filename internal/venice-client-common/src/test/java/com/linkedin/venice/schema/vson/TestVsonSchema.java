package com.linkedin.venice.schema.vson;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonSchema {
  @Test
  public void testVsonSchemaToVschemaStr() {
    String schemaStr =
        "[{\"email\":\"string\", \"metadata\":[{\"key\":\"string\", \"value\":\"string\"}], \"score\":\"float32\"}]";
    VsonSchema schema = VsonSchema.parse(schemaStr);
    Assert.assertTrue(schemaStr.equals(schema.toString()));
  }
}
