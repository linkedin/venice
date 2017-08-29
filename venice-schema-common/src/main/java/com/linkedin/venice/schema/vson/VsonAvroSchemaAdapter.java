package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;


/**
 * generate Avro schema. It will be invoked when creating new stores as Venice needs to convert Vson schema strings to Avro schemas.
 */

public class VsonAvroSchemaAdapter extends AbstractVsonSchemaAdapter<Schema> {
  private static final String DEFAULT_RECORD_NAME = "record";
  private static final String DEFAULT_DOC = null;
  private static final String DEFAULT_NAMESPACE = null;
  private static final JsonNode DEFAULT_VALUE = null;

  public static Schema parse(String vsonSchemaStr) {
    VsonAvroSchemaAdapter adapter = new VsonAvroSchemaAdapter(vsonSchemaStr);
    return adapter.fromVsonObjects();
  }

  private VsonAvroSchemaAdapter(String vsonSchemaStr) {
    super(vsonSchemaStr);
  }

  @Override
  Schema readMap(Map<String, Object> vsonMap) {
    List<Schema.Field> fields = new ArrayList<>();
    vsonMap.forEach((key, value) -> fields.add(new Schema.Field(key, fromVsonObjects(value), DEFAULT_DOC, DEFAULT_VALUE)));

    Schema recordSchema = Schema.createRecord(DEFAULT_RECORD_NAME, DEFAULT_DOC, DEFAULT_NAMESPACE, false);
    recordSchema.setFields(fields);

    return recordSchema;
  }

  @Override
  Schema readList(List<Schema> vsonList){
    return Schema.createArray(fromVsonObjects(vsonList.get(0)));
  }

  @Override
  Schema readPrimitive(String vsonString) {
    Schema avroSchema;
    VsonTypes type = VsonTypes.fromDisplay(vsonString);

    switch(type) {
      case BOOLEAN:
        avroSchema = Schema.create(Schema.Type.BOOLEAN);
        break;
      case STRING:
        avroSchema = Schema.create(Schema.Type.STRING);
        break;
      case INT8:
      case INT16:
      case INT32:
        avroSchema = Schema.create(Schema.Type.INT);
        break;
      case INT64:
        avroSchema = Schema.create(Schema.Type.LONG);
        break;
      case FLOAT32:
        avroSchema = Schema.create(Schema.Type.FLOAT);
        break;
      case FLOAT64:
        avroSchema = Schema.create(Schema.Type.DOUBLE);
        break;
      case BYTES:
        avroSchema = Schema.create(Schema.Type.BYTES);
        break;
      case DATE:
      default:
        throw new VsonSerializationException("Can't parse string to Avro schema. String: "
            + vsonString + " is not a valid Vson String.");
    }

    return avroSchema;
  }
}
