package com.linkedin.venice.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;


public class AvroRecordUtils {
  private AvroRecordUtils() {
  }

  /**
   * This function is used to pre-fill the default value defined in schema.
   * So far, it only support the followings:
   * 1. Only top-level fields are supported.
   * 2. All the primitive types are supported.
   * 3. For union, only `null` default is supported.
   * 4. For array/map, only empty list/map are supported.
   *
   * Other than the above, this function will throw exception.
   *
   * TODO: once the whole stack migrates to the modern avro version (avro-1.7+), we could leverage
   * SpecificRecord builder to prefill the default value, which will be much more powerful.
   * @param recordType
   * @param <T>
   * @return
   */
  public static <T extends SpecificRecord> T prefillAvroRecordWithDefaultValue(T recordType) {
    Schema schema = recordType.getSchema();
    for (Schema.Field field: schema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(field)) {
        // has default
        Object defaultValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case NULL:
            throw new VeniceException("Default value for `null` type is not expected");
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            recordType.put(field.pos(), defaultValue);
            break;
          case UNION:
            if (defaultValue == null) {
              recordType.put(field.pos(), null);
            } else {
              throw new VeniceException("Non 'null' default value is not supported for union type: " + field.name());
            }
            break;
          case ARRAY:
            Collection collection = (Collection) defaultValue;
            if (collection.isEmpty()) {
              recordType.put(field.pos(), new ArrayList<>());
            } else {
              throw new VeniceException(
                  "Non 'empty array' default value is not supported for array type: " + field.name());
            }
            break;
          case MAP:
            Map map = (Map) defaultValue;
            if (map.isEmpty()) {
              recordType.put(field.pos(), new HashMap<>());
            } else {
              throw new VeniceException("Non 'empty map' default value is not supported for map type: " + field.name());
            }
            break;
          case ENUM:
          case FIXED:
          case BYTES:
          case RECORD:
            throw new VeniceException(
                "Default value for field: " + field.name() + " with type: " + fieldType + " is not supported");
        }
      }
    }

    return recordType;
  }

  /**
   * Checks if it is possible for some value to be modified to adapt to the provided schema type.
   * @param expectedSchemaType The type {@link Schema.Type} that the value needs to be adapted to.
   * @return {@code true} if a value can be modified to adapt to the provided schema type; {@code false} otherwise.
   */
  private static boolean canSchemaTypeBeAdapted(Schema.Type expectedSchemaType) {
    switch (expectedSchemaType) {
      case ENUM:
      case FIXED:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
        return false;
      case RECORD:
      case ARRAY:
      case MAP:
      case UNION:
        return true;
      default:
        // Defensive coding
        throw new VeniceException("Unhandled Avro type. This is unexpected.");
    }
  }

  /**
   * Tries to adapt the {@param datum} to the {@param expectedSchema}.
   * The following steps are followed:
   * 1. If the schema type doesn't allow for adaptation, return the same value that was passed in input.
   * 2. If the schema type allows for adaptation, then
   *     2a. If the value doesn't specify a value for any field, the default value is used
   *     2b. If a field is mandatory, but no default values are specified, then an Exception is thrown
   *
   * @param expectedSchema The type {@link Schema} that the value needs to be adapted to.
   * @param datum The value that needs to be adapted to the specified schema
   * @return
   * @throws VeniceException
   */
  public static Object adaptToSchema(Schema expectedSchema, Object datum) throws VeniceException {
    Schema.Type expectedSchemaType = expectedSchema.getType();
    switch (expectedSchemaType) {
      case ENUM:
      case FIXED:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
        return datum;
      case RECORD:
        return adaptRecordToSchema(expectedSchema, datum);
      case ARRAY:
        return adaptListToSchema(expectedSchema, datum);
      case MAP:
        return adaptMapToSchema(expectedSchema, datum);
      case UNION:
        return adaptUnionToSchema(expectedSchema, datum);
      default:
        // Defensive coding
        throw new VeniceException("Unhandled Avro type. This is unexpected.");
    }
  }

  private static Object adaptRecordToSchema(Schema expectedSchema, Object datum) {
    if (!(datum instanceof IndexedRecord)) {
      throw new VeniceException(); // TODO
    }

    IndexedRecord datumRecord = ((IndexedRecord) datum);
    Schema datumSchema = datumRecord.getSchema();

    if (datumSchema.equals(expectedSchema)) {
      return datumRecord;
    }

    RecordDeserializer<IndexedRecord> deserializer =
        FastSerializerDeserializerFactory.getAvroGenericDeserializer(datumSchema, expectedSchema);
    RecordSerializer<IndexedRecord> serializer =
        FastSerializerDeserializerFactory.getAvroGenericSerializer(datumSchema);

    try {
      return deserializer.deserialize(serializer.serialize(datumRecord));
    } catch (VeniceSerializationException e) {
      throw new VeniceException(e);
    }
  }

  private static Collection<?> adaptListToSchema(Schema expectedSchema, Object datum) {
    if (!(datum instanceof Collection)) {
      throw new VeniceException(); // TODO
    }

    Collection<?> datumCollection = (Collection<?>) datum;

    // Adapt List type - adapt all elements
    Schema elementSchema = expectedSchema.getElementType();
    if (AvroRecordUtils.canSchemaTypeBeAdapted(elementSchema.getType())) {
      return datumCollection.stream()
          .map(element -> AvroRecordUtils.adaptToSchema(elementSchema, element))
          .collect(Collectors.toList());
    } else {
      return datumCollection;
    }
  }

  private static Map<String, ?> adaptMapToSchema(Schema expectedSchema, Object datum) {
    if (!(datum instanceof Map)) {
      throw new VeniceException(); // TODO
    }

    // Adapt Map type - adapt all entries
    Map<String, ?> datumMap = (Map<String, ?>) datum;
    Schema elementSchema = expectedSchema.getValueType();
    if (AvroRecordUtils.canSchemaTypeBeAdapted(elementSchema.getType())) {
      return datumMap.entrySet()
          .stream()
          .collect(
              Collectors
                  .toMap(Map.Entry::getKey, entry -> AvroRecordUtils.adaptToSchema(elementSchema, entry.getValue())));
    } else {
      return datumMap;
    }
  }

  private static Object adaptUnionToSchema(Schema expectedSchema, Object datum) {
    int index;
    try {
      index = GenericData.get().resolveUnion(expectedSchema, datum);
    } catch (Exception e) {
      throw new VeniceException(e);
    }
    return adaptToSchema(expectedSchema.getTypes().get(index), datum);
  }
}
