package com.linkedin.venice.helix;

import com.linkedin.venice.schema.SchemaEntry;
import java.io.IOException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchemaEntrySerializer {
  @Test
  public void testSerialize() throws IOException {
    String schemaStr = "{\n" + "  \"namespace\": \"example.avro\",\n" + "  \"type\": \"record\",\n"
        + "  \"name\": \"User\",\n" + "  \"fields\": [\n" + "    {\"name\": \"name\", \"type\": \"string\"},\n"
        + "    {\n" + "      \"name\": \"kind\",\n" + "      \"type\": {\n" + "        \"name\": \"Kind\",\n"
        + "        \"type\": \"enum\",\n" + "        \"symbols\" : [\"ONE\", \"TWO\", \"THREE\"]\n" + "      }\n"
        + "    }\n" + "  ]\n" + "}";
    int id = 1;
    SchemaEntry entry = new SchemaEntry(id, schemaStr);
    SchemaEntrySerializer serializer = new SchemaEntrySerializer();
    String expected =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"kind\",\"type\":{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"ONE\",\"TWO\",\"THREE\"]}}]}";
    Assert.assertEquals(new String(serializer.serialize(entry, "/test_store/value_schema/1")), expected);
  }

  @Test
  public void testDeserialize() throws IOException {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"kind\",\"type\":{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"ONE\",\"TWO\",\"THREE\"]}}]}";
    String path = "/test_store/value_schema/1";
    SchemaEntrySerializer serializer = new SchemaEntrySerializer();

    SchemaEntry entry = serializer.deserialize(schemaStr.getBytes(), path);
    Assert.assertEquals(entry.getId(), 1);
    Schema schema = entry.getSchema();
    Assert.assertEquals(schema.getName(), "User");
    Assert.assertNotNull(schema.getField("name"));
    Assert.assertNotNull(schema.getField("kind"));
  }
}
