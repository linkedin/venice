package com.linkedin.venice.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class RandomAvroObjectGenerator {

  private final Schema schema;
  private final Random random;

  public RandomAvroObjectGenerator(Schema schema, Random random) {
    this.schema = schema;
    this.random = random;
  }

  public Object generate() {
    return generateObject(schema);
  }

  private Object generateObject(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return generateArray(schema);
      case BOOLEAN:
        return generateBoolean();
      case BYTES:
        return generateBytes();
      case DOUBLE:
        return generateDouble();
      case ENUM:
        return generateEnumSymbol(schema);
      case FIXED:
        return generateFixed(schema);
      case FLOAT:
        return generateFloat();
      case INT:
        return generateInt();
      case LONG:
        return generateLong();
      case MAP:
        return generateMap(schema);
      case NULL:
        return generateNull();
      case RECORD:
        return generateRecord(schema);
      case STRING:
        return generateString();
      case UNION:
        return generateUnion(schema);
      default:
        throw new RuntimeException("Unrecognized schema type: " + schema.getType());
    }
  }

  private Collection<Object> generateArray(Schema schema) {
    // TODO: It'd be swell to make length configurable
    int length = random.nextInt(50);
    Collection<Object> result = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      result.add(generateObject(schema.getElementType()));
    }
    return result;
  }

  private Boolean generateBoolean() {
    return random.nextBoolean();
  }

  private ByteBuffer generateBytes() {
    byte[] bytes = new byte[100];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  private Double generateDouble() {
    return random.nextDouble();
  }

  private GenericEnumSymbol generateEnumSymbol(Schema schema) {
    List<String> enums = schema.getEnumSymbols();
    return AvroCompatibilityHelper.newEnumSymbol(schema, enums.get(random.nextInt(enums.size())));
  }

  private GenericFixed generateFixed(Schema schema) {
    byte[] bytes = new byte[schema.getFixedSize()];
    random.nextBytes(bytes);
    return AvroCompatibilityHelper.newFixed(schema, bytes);
  }

  private Float generateFloat() {
    return random.nextFloat();
  }

  private Integer generateInt() {
    return random.nextInt();
  }

  private Long generateLong() {
    return random.nextLong();
  }

  private Map<String, Object> generateMap(Schema schema) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      result.put(generateRandomString(1), generateObject(schema.getValueType()));
    }
    return result;
  }

  private Object generateNull() {
    return null;
  }

  private GenericRecord generateRecord(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      record.put(field.name(), generateObject(field.schema()));
    }
    return record;
  }

  private String generateRandomString(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) random.nextInt(128);
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private String generateString() {
    return generateRandomString(10);
  }

  private Object generateUnion(Schema schema) {
    List<Schema> schemas = schema.getTypes();
    return generateObject(schemas.get(random.nextInt(schemas.size())));
  }
}
