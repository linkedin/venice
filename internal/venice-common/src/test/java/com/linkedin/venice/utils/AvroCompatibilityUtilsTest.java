package com.linkedin.venice.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityUtilsTest {
  @Test
  public void testWithDifferentDocField() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},   {\"name\": \"additional\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n]}";
    Schema schema = Schema.parse(schemaStr1);
    GenericData.Record o1 = new GenericData.Record(schema);
    GenericData.Record o2 = new GenericData.Record(schema);
    o1.put("name", "good");
    o2.put("name", "good");
    Map map = new HashMap();
    map.put("1", 1);
    map.put("2", 2);
    o1.put("additional", map);
    o2.put("additional", map);
    Assert.assertTrue(AvroCompatibilityUtils.compareGenericData(o1, o2, schema) == 0);

    Map map1 = new HashMap();
    map1.put("1", 1);
    map1.put("2", 10);
    o2.put("additional", map1);
    Assert.assertTrue(AvroCompatibilityUtils.compareGenericData(o1, o2, schema) != 0);

    map.put("2", 10);
    Assert.assertTrue(AvroCompatibilityUtils.compareGenericData(o1, o2, schema) == 0);
  }
}
