package com.linkedin.venice.schema.vson;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.VsonSerializationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * generate Avro schema. It will be invoked when creating new stores as Venice needs to convert Vson schema strings to Avro schemas.
 */
@Deprecated
public class VsonAvroSchemaAdapter extends AbstractVsonSchemaAdapter<Schema> {
  private static final String DEFAULT_RECORD_NAME = "record";
  public static final String DEFAULT_DOC = null;
  public static final String DEFAULT_NAMESPACE = null;
  private static final Object DEFAULT_VALUE = null;

  // both vson int8 and int16 would be wrapped as Avro fixed.
  public static final String BYTE_WRAPPER = "byteWrapper";
  public static final String SHORT_WRAPPER = "shortWrapper";

  private int recordCount = 1;

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
    vsonMap.forEach(
        (key, value) -> fields.add(
            AvroCompatibilityHelper.newField(null)
                .setName(key)
                .setSchema(fromVsonObjects(value))
                .setOrder(Schema.Field.Order.ASCENDING)
                .setDoc(DEFAULT_DOC)
                .setDefault(DEFAULT_VALUE)
                .build()));

    Schema recordSchema =
        Schema.createRecord(DEFAULT_RECORD_NAME + (recordCount++), DEFAULT_DOC, DEFAULT_NAMESPACE, false);
    recordSchema.setFields(fields);

    return nullableUnion(recordSchema);
  }

  @Override
  Schema readList(List<Schema> vsonList) {
    return nullableUnion(Schema.createArray(fromVsonObjects(vsonList.get(0))));
  }

  @Override
  Schema readPrimitive(String vsonString) {
    Schema avroSchema;
    VsonTypes type = VsonTypes.fromDisplay(vsonString);

    switch (type) {
      case BOOLEAN:
        avroSchema = nullableUnion(Schema.create(Schema.Type.BOOLEAN));
        break;
      case STRING:
        avroSchema = nullableUnion(Schema.create(Schema.Type.STRING));
        break;
      case INT32:
        avroSchema = nullableUnion(Schema.create(Schema.Type.INT));
        break;
      case INT64:
        avroSchema = nullableUnion(Schema.create(Schema.Type.LONG));
        break;
      case FLOAT32:
        avroSchema = nullableUnion(Schema.create(Schema.Type.FLOAT));
        break;
      case FLOAT64:
        avroSchema = nullableUnion(Schema.create(Schema.Type.DOUBLE));
        break;
      case BYTES:
        avroSchema = nullableUnion(Schema.create(Schema.Type.BYTES));
        break;
      case INT8:
        avroSchema = nullableUnion(constructFixedType(BYTE_WRAPPER, 1));
        break;
      case INT16:
        avroSchema = nullableUnion(constructFixedType(SHORT_WRAPPER, 2));
        break;
      case DATE:
        throw new VsonSerializationException("Vson type: " + type.toDisplay() + " is not supported to convert to Avro");
      default:
        throw new VsonSerializationException(
            "Can't parse string to Avro schema. String: " + vsonString + " is not a valid Vson String.");
    }

    return avroSchema;
  }

  private Schema constructFixedType(String name, int size) {
    return Schema.createFixed(name, DEFAULT_DOC, DEFAULT_NAMESPACE, size);
  }

  /**
   * Wrap a schema with a union so that it could return null if necessary
   * From Vson's point of view, all fields can be optional (null is allowed to be return).
   */
  public static Schema nullableUnion(Schema schema) {
    return Schema.createUnion(Arrays.asList(schema, Schema.create(Schema.Type.NULL)));
  }

  public static Schema stripFromUnion(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      throw new IllegalArgumentException("Schema: " + schema + " has to be Union");
    }

    List<Schema> subtypes = schema.getTypes();

    return subtypes.get(0).getType() != Schema.Type.NULL ? subtypes.get(0) : subtypes.get(1);
  }
}
