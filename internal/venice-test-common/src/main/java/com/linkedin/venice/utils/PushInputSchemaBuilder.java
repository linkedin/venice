package com.linkedin.venice.utils;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * This class is a simple schema builder to generate Venice Push Job input file schema. This builder allows user to set
 * up key schema, value schema and additional unrelated fields. It will perform sanity check to make sure generated schema
 * is good for push job in integration test.
 */
public class PushInputSchemaBuilder {
  private static final String namespace = "example.avro";
  private static final String name = "AvroFileRecord";
  private static final String doc = "File Schema For Test Push";
  private final Schema fileSchema = Schema.createRecord(name, doc, namespace, false);
  private final Map<String, Schema.Field> nameToFieldMap = new HashMap<>();

  public PushInputSchemaBuilder() {
  }

  public PushInputSchemaBuilder setKeySchema(Schema schema) {
    return setFieldSchema(DEFAULT_KEY_FIELD_PROP, schema);
  }

  public PushInputSchemaBuilder setValueSchema(Schema schema) {
    return setFieldSchema(DEFAULT_VALUE_FIELD_PROP, schema);
  }

  public PushInputSchemaBuilder setFieldSchema(String fieldName, Schema fieldSchema) {
    if (nameToFieldMap.containsKey(fieldName)) {
      throw new IllegalStateException(
          "Field has been set: " + fieldName + " with schema: " + nameToFieldMap.get(fieldName).toString());
    }
    nameToFieldMap.put(fieldName, AvroCompatibilityHelper.createSchemaField(fieldName, fieldSchema, "", null));
    return this;
  }

  public Schema build() {
    if (!nameToFieldMap.containsKey(DEFAULT_KEY_FIELD_PROP)) {
      throw new IllegalStateException("Key field schema has not been setup.");
    }
    if (!nameToFieldMap.containsKey(DEFAULT_VALUE_FIELD_PROP)) {
      throw new IllegalStateException("Value field schema has not been setup.");
    }
    fileSchema.setFields(new ArrayList<>(nameToFieldMap.values()));
    return fileSchema;
  }
}
