package com.linkedin.venice.hadoop.spark.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.ByteUtils;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;


/**
 * https://spark.apache.org/docs/latest/sql-data-sources-avro.html
 * https://spark.apache.org/docs/latest/sql-ref-datatypes.html
 * https://avro.apache.org/docs/1.11.1/specification/
 *
 * Convert Row -> Avro Record. Not convert StructType -> Avro Schema
 *
 * The scope is not the full Spark catalyst types. But only the ones that are supported by Avro.
 * Eventually, we probably need to support the full range of Spark types.
 */
public class RowToAvroConverter {
  private static LogicalTypes.Decimal TYPE_DECIMAL = LogicalTypes.decimal(0);
  private static final Conversions.DecimalConversion DECIMAL_CONVERTER = new Conversions.DecimalConversion();

  public static GenericRecord convert(Row row, Schema schema) {
    Validate.notNull(row, "Row must not be null");
    Validate.notNull(schema, "Schema must not be null");
    Validate
        .isTrue(schema.getType().equals(Schema.Type.RECORD), "Schema must be of type RECORD. Got: " + schema.getType());
    Validate.isInstanceOf(Row.class, row, "Row must be of type Row. Got: " + row.getClass().getName());

    return convertToRecord(row, row.schema(), schema);
  }

  private static GenericRecord convertToRecord(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(StructType.class, dataType, "Expected StructType, got: " + dataType.getClass().getName());
    Validate.isInstanceOf(Row.class, o, "Expected Row, got: " + o.getClass().getName());
    GenericRecord aResult = new GenericData.Record(schema);

    Row row = (Row) o;

    StructType sType = row.schema();
    StructField[] sFields = sType.fields();
    List<Schema.Field> aFields = schema.getFields();

    Validate.isTrue(
        sFields.length == aFields.size(),
        "Row and Avro schema must have the same number of fields. Row: " + sFields.length + ", Avro: "
            + aFields.size());

    for (int i = 0; i < sFields.length; i++) {
      StructField structField = sFields[i];
      Schema.Field avroField = aFields.get(i);

      // Spark field names are case-insensitive
      Validate.isTrue(
          structField.name().equalsIgnoreCase(avroField.name()),
          "Field names must match. Row: " + structField.name() + ", Avro: " + avroField.name());

      Object elem = row.get(i);
      aResult.put(i, convert(elem, structField.dataType(), avroField.schema()));
    }

    return aResult;
  }

  private static Boolean convertToBoolean(Object o, DataType dataType) {
    Validate.isInstanceOf(BooleanType.class, dataType, "Expected BooleanType, got: " + dataType.getClass().getName());
    Validate.isInstanceOf(Boolean.class, o, "Expected Boolean, got: " + o.getClass().getName());
    return ((Boolean) o);
  }

  private static Integer convertToInt(Object o, DataType dataType, Schema schema) {
    // IntegerType
    if (dataType instanceof IntegerType) {
      Validate.isInstanceOf(Integer.class, o, "Expected Integer, got: " + o.getClass().getName());
      return ((Integer) o);
    }

    // Avro logical type "date" is read as DateType in Spark
    if (dataType instanceof DateType) {
      validateLogicalType(schema, LogicalTypes.date());

      LocalDate localDate;

      if (o instanceof LocalDate) {
        localDate = ((LocalDate) o);
      } else if (o instanceof Date) {
        localDate = ((Date) o).toLocalDate();
      } else {
        throw new IllegalArgumentException(
            "Unsupported date type: " + o.getClass().getName() + ". Expected java.time.LocalDate or java.sql.Date");
      }

      // Long to int, but we are sure that it fits
      return (int) localDate.toEpochDay();
    }

    // ByteType
    if (dataType instanceof ByteType) {
      Validate.isInstanceOf(Byte.class, o, "Expected Integer, got: " + o.getClass().getName());
      return ((Byte) o).intValue();
    }

    // ShortType
    if (dataType instanceof ShortType) {
      Validate.isInstanceOf(Short.class, o, "Expected Integer, got: " + o.getClass().getName());
      return ((Short) o).intValue();
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  private static Long convertToLong(Object o, DataType dataType, Schema schema) {
    // LongType
    if (dataType instanceof LongType) {
      Validate.isInstanceOf(Long.class, o, "Expected Long, got: " + o.getClass().getName());
      return ((Long) o);
    }

    // Avro logical types "timestamp-millis" and "timestamp-micros" are read as LongType in Spark
    if (dataType instanceof TimestampType) {
      LogicalType logicalType = validateLogicalType(schema, LogicalTypes.timeMicros(), LogicalTypes.timeMillis());

      Instant instant;
      if (o instanceof java.time.Instant) {
        instant = ((java.time.Instant) o);
      } else if (o instanceof java.sql.Timestamp) {
        instant = ((java.sql.Timestamp) o).toInstant();
      } else {
        throw new IllegalArgumentException(
            "Unsupported timestamp type: " + o.getClass().getName()
                + ". Expected java.time.Instant or java.sql.Timestamp");
      }

      long nanos = instant.getNano();
      long micros = nanos / 1000;

      if (logicalType == LogicalTypes.timeMicros()) {
        return instant.getEpochSecond() * 1000 * 1000 + micros;
      }

      return instant.toEpochMilli();
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  private static Float convertToFloat(Object o, DataType dataType) {
    Validate.isInstanceOf(FloatType.class, dataType, "Expected FloatType, got: " + dataType);
    Validate.isInstanceOf(Float.class, o, "Expected Float, got: " + o.getClass().getName());
    return ((Float) o);
  }

  private static Double convertToDouble(Object o, DataType dataType) {
    Validate.isInstanceOf(DoubleType.class, dataType, "Expected DoubleType, got: " + dataType);
    Validate.isInstanceOf(Double.class, o, "Expected Double, got: " + o.getClass().getName());
    return ((Double) o);
  }

  private static CharSequence convertToString(Object o, DataType dataType) {
    Validate.isInstanceOf(StringType.class, dataType, "Expected StringType, got: " + dataType);
    Validate.isInstanceOf(CharSequence.class, o, "Expected CharSequence, got: " + o.getClass().getName());
    return ((CharSequence) o);
  }

  private static ByteBuffer convertToBytes(Object o, DataType dataType, Schema schema) {
    if (dataType instanceof BinaryType) {
      if (o instanceof byte[]) {
        return ByteBuffer.wrap((byte[]) o);
      }

      if (o instanceof ByteBuffer) {
        return (ByteBuffer) o;
      }

      throw new IllegalArgumentException("Unsupported byte array type: " + o.getClass().getName());
    }

    if (dataType instanceof DecimalType) {
      validateLogicalType(schema, TYPE_DECIMAL);
      Validate.isInstanceOf(BigDecimal.class, o, "Expected BigDecimal, got: " + o.getClass().getName());
      BigDecimal decimal = (BigDecimal) o;
      LogicalTypes.Decimal l = (LogicalTypes.Decimal) schema.getLogicalType();
      return DECIMAL_CONVERTER.toBytes(decimal, schema, l);
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  private static GenericFixed convertToFixed(Object o, DataType dataType, Schema schema) {
    if (dataType instanceof BinaryType) {
      if (o instanceof byte[]) {
        byte[] bytes = (byte[]) o;
        Validate.isTrue(
            bytes.length == schema.getFixedSize(),
            "Fixed size mismatch. Expected: " + schema.getFixedSize() + ", got: " + bytes.length);
        return new GenericData.Fixed(schema, bytes);
      }

      if (o instanceof ByteBuffer) {
        ByteBuffer bytes = (ByteBuffer) o;
        Validate.isTrue(
            bytes.remaining() == schema.getFixedSize(),
            "Fixed size mismatch. Expected: " + schema.getFixedSize() + ", got: " + bytes.remaining());
        return new GenericData.Fixed(schema, ByteUtils.extractByteArray(bytes));
      }

      throw new IllegalArgumentException("Unsupported byte array type: " + o.getClass().getName());
    }

    if (dataType instanceof DecimalType) {
      validateLogicalType(schema, TYPE_DECIMAL);
      Validate.isInstanceOf(BigDecimal.class, o, "Expected BigDecimal, got: " + o.getClass().getName());
      BigDecimal decimal = (BigDecimal) o;
      Conversions.DecimalConversion DECIMAL_CONVERTER = new Conversions.DecimalConversion();
      LogicalTypes.Decimal l = (LogicalTypes.Decimal) schema.getLogicalType();
      return DECIMAL_CONVERTER.toFixed(decimal, schema, l);
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  private static GenericData.EnumSymbol convertToEnum(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(StringType.class, dataType, "Expected StringType, got: " + dataType);
    Validate.isInstanceOf(CharSequence.class, o, "Expected CharSequence, got: " + o.getClass().getName());
    Validate.isTrue(schema.getEnumSymbols().contains(o.toString()), "Enum symbol not found: " + o);
    return AvroCompatibilityHelper.newEnumSymbol(schema, ((CharSequence) o).toString());
  }

  private static List<Object> convertToArray(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(ArrayType.class, dataType, "Expected ArrayType, got: " + dataType);
    Validate
        .isInstanceOf(scala.collection.Seq.class, o, "Expected scala.collection.Seq, got: " + o.getClass().getName());

    // Type of elements in the array
    Schema elementType = schema.getElementType();

    List inputList = scala.collection.JavaConversions.seqAsJavaList((scala.collection.Seq) o);
    List<Object> outputList = new ArrayList<>(inputList.size());

    for (Object element: inputList) {
      outputList.add(convert(element, ((ArrayType) dataType).elementType(), elementType));
    }

    return outputList;
  }

  private static Map<CharSequence, Object> convertToMap(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(MapType.class, dataType, "Expected MapType, got: " + dataType.getClass().getName());
    Validate
        .isInstanceOf(scala.collection.Map.class, o, "Expected scala.collection.Map, got: " + o.getClass().getName());

    MapType sType = ((MapType) dataType);

    Map inputMap = scala.collection.JavaConversions.mapAsJavaMap((scala.collection.Map) o);
    Map<CharSequence, Object> outputMap = new HashMap<>(inputMap.size());

    for (Object entryObj: inputMap.entrySet()) {
      Validate.isInstanceOf(Map.Entry.class, entryObj, "Expected Map.Entry, got: " + entryObj.getClass().getName());
      Map.Entry entry = (Map.Entry) entryObj;
      outputMap.put(
          // Key is always a String in Avro
          convertToString(entry.getKey(), sType.keyType()),
          convert(entry.getValue(), sType.valueType(), schema.getValueType()));
    }

    return outputMap;
  }

  private static Object convertToUnion(Object o, DataType dataType, Schema schema) {
    if (o == null) {
      Validate.isTrue(schema.isNullable(), "Field is not nullable: " + schema.getName());
      return null;
    }

    List<Schema> types = schema.getTypes();
    Schema first = types.get(0);
    // If there's only one branch, Spark will use that as the data type
    if (types.size() == 1) {
      return convert(o, dataType, first);
    }

    Schema second = types.get(1);
    if (types.size() == 2) {
      // If there are only two branches, and one of them is null, Spark will use the non-null type
      if (first.getType() == Schema.Type.NULL) {
        return convert(o, dataType, second);
      }

      if (second.getType() == Schema.Type.NULL) {
        return convert(o, dataType, first);
      }

      // A union of int and long is read as LongType.
      // This is lossy because we cannot know what type was provided in the input
      if ((first.getType() == Schema.Type.INT && second.getType() == Schema.Type.LONG)
          || (first.getType() == Schema.Type.LONG && second.getType() == Schema.Type.INT)) {
        return convertToLong(o, dataType, schema);
      }

      // A union of float and double is read as DoubleType.
      // This is lossy because we cannot know what type was provided in the input
      if ((first.getType() == Schema.Type.FLOAT && second.getType() == Schema.Type.DOUBLE)
          || (first.getType() == Schema.Type.DOUBLE && second.getType() == Schema.Type.FLOAT)) {
        return convertToDouble(o, dataType);
      }
    }

    // Now, handle complex unions: member0, member1, ...
    // TODO(nithakka): Handle complex union types: If a branch of the union is "null", then it is skipped in the
    // Catalyst schema.
    // So, [ "null", "int", "string" ], [ "int", "null", "string" ], [ "int", "string", "null" ], will all be parsed as
    // member0 -> IntegerType, member1 -> StringType.
    Validate.isInstanceOf(StructType.class, dataType, "Expected StructType, got: " + dataType.getClass().getName());
    Validate.isInstanceOf(Row.class, o, "Expected Row, got: " + o.getClass().getName());
    Row row = (Row) o;

    StructType structType = (StructType) dataType;
    StructField[] structFields = structType.fields();
    int structFieldIndex = 0;
    for (Schema type: types) {
      if (type.getType() == Schema.Type.NULL) {
        // Union branch "null" is not handled in the Catalyst schema
        continue;
      }
      Object unionField = row.get(structFieldIndex);
      if (unionField != null) {
        return convert(unionField, structFields[structFieldIndex].dataType(), type);
      }
      structFieldIndex++;
    }

    throw new IllegalArgumentException("Expected union of null and another type, got: " + types);
  }

  private static Object convert(Object o, DataType dataType, Schema schema) {
    if (o == null) {
      Validate.isTrue(schema.isNullable(), "Field is not nullable: " + schema.getName());
      return null;
    }

    switch (schema.getType()) {
      case BOOLEAN:
        return convertToBoolean(o, dataType);
      case INT:
        return convertToInt(o, dataType, schema);
      case LONG:
        return convertToLong(o, dataType, schema);
      case FLOAT:
        return convertToFloat(o, dataType);
      case DOUBLE:
        return convertToDouble(o, dataType);
      case STRING:
        return convertToString(o, dataType);
      case BYTES:
        return convertToBytes(o, dataType, schema);
      case FIXED:
        return convertToFixed(o, dataType, schema);
      case ENUM:
        return convertToEnum(o, dataType, schema);
      case ARRAY:
        return convertToArray(o, dataType, schema);
      case MAP:
        return convertToMap(o, dataType, schema);
      case RECORD:
        return convertToRecord(o, dataType, schema);
      case UNION:
        return convertToUnion(o, dataType, schema);
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + schema.getType());
    }
  }

  private static LogicalType validateLogicalType(Schema schema, LogicalType... expectedTypes) {
    LogicalType logicalType = schema.getLogicalType();
    Validate.notNull(logicalType, "Expected Avro logical type to be present, got: " + logicalType);

    for (LogicalType expectedType: expectedTypes) {
      // Decimal is not a singleton
      if (expectedType instanceof LogicalTypes.Decimal) {
        return expectedType;
      }

      if (expectedType == logicalType) {
        return expectedType;
      }
    }

    throw new IllegalArgumentException(
        "Expected Avro logical type to be one of: " + expectedTypes + ", got: " + logicalType);
  }
}
