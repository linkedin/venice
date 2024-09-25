package com.linkedin.venice.spark.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.ByteUtils;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
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
import org.apache.spark.sql.types.DayTimeIntervalType;
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
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import scala.collection.JavaConverters;


/**
 * A utility class to convert Spark SQL Row to an Avro GenericRecord with the specified schema. This has been written in
 * accordance with the following resources:
 * <ul>
 *   <li><a href="https://spark.apache.org/docs/latest/sql-data-sources-avro.html">Spark Avro data source documentation</a></li>
 *   <li><a href="https://avro.apache.org/docs/1.11.1/spec.html">Avro specification</a></li>
 *   <li><a href="https://github.com/apache/spark/blob/master/connector/avro/src/main/scala/org/apache/spark/sql/avro/AvroSerializer.scala">Spark's internal Catalyst type to Avro bytes implementation</a></li>
 * </ul>
 *
 * Spark's implementation is not ideal to be used directly for two reasons:
 * <ul>
 *   <li>It cannot handle complex unions in the version of Spark that we use (3.3.3). The support was added in 3.4.0.</li>
 *   <li>It converts directly to Avro binary that we need to deserialize, and that incurs an additional serde cost.</li>
 * </ul>
 */
public final class RowToAvroConverter {
  private RowToAvroConverter() {
  }

  private static final Conversions.DecimalConversion DECIMAL_CONVERTER = new Conversions.DecimalConversion();

  public static GenericRecord convert(Row row, Schema schema) {
    Validate.notNull(row, "Row must not be null");
    Validate.notNull(schema, "Schema must not be null");
    Validate
        .isTrue(schema.getType().equals(Schema.Type.RECORD), "Schema must be of type RECORD. Got: " + schema.getType());
    Validate.isInstanceOf(Row.class, row, "Row must be of type Row. Got: " + row.getClass().getName());

    return convertToRecord(row, row.schema(), schema);
  }

  static GenericRecord convertToRecord(Object o, DataType dataType, Schema schema) {
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
      aResult.put(i, convertInternal(elem, structField.dataType(), avroField.schema()));
    }

    return aResult;
  }

  static Boolean convertToBoolean(Object o, DataType dataType) {
    Validate.isInstanceOf(BooleanType.class, dataType, "Expected BooleanType, got: " + dataType.getClass().getName());
    Validate.isInstanceOf(Boolean.class, o, "Expected Boolean, got: " + o.getClass().getName());
    return ((Boolean) o);
  }

  static Integer convertToInt(Object o, DataType dataType, Schema schema) {
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

    if (dataType instanceof ByteType) {
      Validate.isInstanceOf(Byte.class, o, "Expected Integer, got: " + o.getClass().getName());
      return ((Byte) o).intValue();
    }

    if (dataType instanceof ShortType) {
      Validate.isInstanceOf(Short.class, o, "Expected Integer, got: " + o.getClass().getName());
      return ((Short) o).intValue();
    }

    // Spark default Avro converter converts YearMonthIntervalType to int type
    // This is not the type read by Spark's native Avro reader, but added to support YearMonthIntervalType
    if (dataType instanceof YearMonthIntervalType) {
      Validate.isInstanceOf(Period.class, o, "Expected Period, got: " + o.getClass().getName());
      return ((Period) o).getMonths();
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  static Long convertToLong(Object o, DataType dataType, Schema schema) {
    // LongType
    if (dataType instanceof LongType) {
      Validate.isInstanceOf(Long.class, o, "Expected Long, got: " + o.getClass().getName());
      return ((Long) o);
    }

    // Avro logical types "timestamp-millis" and "timestamp-micros" are read as LongType in Spark
    if (dataType instanceof TimestampType) {
      LogicalType logicalType =
          validateLogicalType(schema, false, LogicalTypes.timestampMicros(), LogicalTypes.timestampMillis());

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

      if (logicalType == null || logicalType == LogicalTypes.timestampMillis()) {
        return ChronoUnit.MILLIS.between(Instant.EPOCH, instant);
      }

      return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }

    // Spark default Avro converter converts TimestampNTZType to int type
    // This is not the type read by Spark's native Avro reader, but added to support TimestampNTZType
    // Avro logical types "local-timestamp-millis" and "local-timestamp-micros" are read as LongType in Spark
    if (dataType instanceof TimestampNTZType) {
      LogicalType logicalType =
          validateLogicalType(schema, false, LogicalTypes.localTimestampMicros(), LogicalTypes.localTimestampMillis());
      Validate.isInstanceOf(java.time.LocalDateTime.class, o, "Expected LocalDateTime, got: " + o.getClass().getName());

      LocalDateTime localDateTime = ((java.time.LocalDateTime) o);
      LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);

      if (logicalType == null || logicalType == LogicalTypes.localTimestampMillis()) {
        return ChronoUnit.MILLIS.between(epoch, localDateTime);
      }

      return ChronoUnit.MICROS.between(epoch, localDateTime);
    }

    // Spark default Avro converter converts DayTimeIntervalType to long type
    // This is not the type read by Spark's native Avro reader, but added to support DayTimeIntervalType
    if (dataType instanceof DayTimeIntervalType) {
      Validate.isInstanceOf(Duration.class, o, "Expected Duration, got: " + o.getClass().getName());
      Duration duration = (Duration) o;
      return TimeUnit.SECONDS.toMicros(duration.getSeconds()) + TimeUnit.NANOSECONDS.toMicros(duration.getNano());
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  static Float convertToFloat(Object o, DataType dataType) {
    Validate.isInstanceOf(FloatType.class, dataType, "Expected FloatType, got: " + dataType);
    Validate.isInstanceOf(Float.class, o, "Expected Float, got: " + o.getClass().getName());
    return ((Float) o);
  }

  static Double convertToDouble(Object o, DataType dataType) {
    Validate.isInstanceOf(DoubleType.class, dataType, "Expected DoubleType, got: " + dataType);
    Validate.isInstanceOf(Double.class, o, "Expected Double, got: " + o.getClass().getName());
    return ((Double) o);
  }

  static CharSequence convertToString(Object o, DataType dataType) {
    Validate.isInstanceOf(StringType.class, dataType, "Expected StringType, got: " + dataType);
    Validate.isInstanceOf(CharSequence.class, o, "Expected CharSequence, got: " + o.getClass().getName());
    return ((CharSequence) o);
  }

  static ByteBuffer convertToBytes(Object o, DataType dataType, Schema schema) {
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
      DecimalType decimalType = (DecimalType) dataType;
      validateLogicalType(schema, LogicalTypes.decimal(decimalType.precision(), decimalType.scale()));
      Validate.isInstanceOf(BigDecimal.class, o, "Expected BigDecimal, got: " + o.getClass().getName());
      BigDecimal decimal = (BigDecimal) o;
      LogicalTypes.Decimal l = (LogicalTypes.Decimal) schema.getLogicalType();
      return DECIMAL_CONVERTER.toBytes(decimal, schema, l);
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  static GenericFixed convertToFixed(Object o, DataType dataType, Schema schema) {
    if (dataType instanceof BinaryType) {
      if (o instanceof byte[]) {
        byte[] bytes = (byte[]) o;
        Validate.isTrue(
            bytes.length == schema.getFixedSize(),
            "Fixed size mismatch. Expected: " + schema.getFixedSize() + ", got: " + bytes.length);
        return AvroCompatibilityHelper.newFixed(schema, bytes);
      }

      if (o instanceof ByteBuffer) {
        ByteBuffer bytes = (ByteBuffer) o;
        Validate.isTrue(
            bytes.remaining() == schema.getFixedSize(),
            "Fixed size mismatch. Expected: " + schema.getFixedSize() + ", got: " + bytes.remaining());
        return AvroCompatibilityHelper.newFixed(schema, ByteUtils.extractByteArray(bytes));
      }

      throw new IllegalArgumentException("Unsupported byte array type: " + o.getClass().getName());
    }

    if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      validateLogicalType(schema, LogicalTypes.decimal(decimalType.precision(), decimalType.scale()));
      Validate.isInstanceOf(BigDecimal.class, o, "Expected BigDecimal, got: " + o.getClass().getName());
      BigDecimal decimal = (BigDecimal) o;
      Conversions.DecimalConversion DECIMAL_CONVERTER = new Conversions.DecimalConversion();
      LogicalTypes.Decimal l = (LogicalTypes.Decimal) schema.getLogicalType();
      return DECIMAL_CONVERTER.toFixed(decimal, schema, l);
    }

    throw new IllegalArgumentException("Unsupported data type: " + dataType);
  }

  static GenericEnumSymbol convertToEnum(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(StringType.class, dataType, "Expected StringType, got: " + dataType);
    Validate.isInstanceOf(CharSequence.class, o, "Expected CharSequence, got: " + o.getClass().getName());
    Validate.isTrue(schema.getEnumSymbols().contains(o.toString()), "Enum symbol not found: " + o);
    return AvroCompatibilityHelper.newEnumSymbol(schema, ((CharSequence) o).toString());
  }

  static List<Object> convertToArray(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(ArrayType.class, dataType, "Expected ArrayType, got: " + dataType);

    // Type of elements in the array
    Schema elementType = schema.getElementType();

    List inputList;
    if (o instanceof List) {
      inputList = (List) o;
    } else if (o instanceof scala.collection.Seq) {
      // If the input is a scala.collection.Seq, convert it to a List
      inputList = JavaConverters.seqAsJavaList((scala.collection.Seq) o);
    } else {
      throw new IllegalArgumentException("Unsupported array type: " + o.getClass().getName());
    }

    List<Object> outputList = new ArrayList<>(inputList.size());

    for (Object element: inputList) {
      outputList.add(convertInternal(element, ((ArrayType) dataType).elementType(), elementType));
    }

    return outputList;
  }

  static Map<CharSequence, Object> convertToMap(Object o, DataType dataType, Schema schema) {
    Validate.isInstanceOf(MapType.class, dataType, "Expected MapType, got: " + dataType.getClass().getName());

    MapType sType = ((MapType) dataType);

    Map inputMap;
    if (o instanceof Map) {
      inputMap = (Map) o;
    } else if (o instanceof scala.collection.Map) {
      inputMap = JavaConverters.mapAsJavaMap((scala.collection.Map) o);
    } else {
      throw new IllegalArgumentException("Unsupported map type: " + o.getClass().getName());
    }

    Map<CharSequence, Object> outputMap = new HashMap<>(inputMap.size());

    for (Object entryObj: inputMap.entrySet()) {
      Validate.isInstanceOf(Map.Entry.class, entryObj, "Expected Map.Entry, got: " + entryObj.getClass().getName());
      Map.Entry entry = (Map.Entry) entryObj;
      outputMap.put(
          // Key is always a String in Avro
          convertToString(entry.getKey(), sType.keyType()),
          convertInternal(entry.getValue(), sType.valueType(), schema.getValueType()));
    }

    return outputMap;
  }

  static Object convertToUnion(Object o, DataType dataType, Schema schema) {
    if (o == null) {
      Validate.isTrue(schema.isNullable(), "Field is not nullable: " + schema.getName());
      return null;
    }

    // Now that we've checked for null explicitly, we should process everything else as a non-null value.
    // This is consistent with the way Spark handles unions.
    List<Schema> types =
        schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).collect(Collectors.toList());
    Schema first = types.get(0);
    // If there's only one branch, Spark will use that as the data type
    if (types.size() == 1) {
      return convertInternal(o, dataType, first);
    }

    Schema second = types.get(1);
    if (types.size() == 2) {
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
    // If a branch of the union is "null", then it is skipped in the Catalyst schema.
    // So, [ "null", "int", "string" ], [ "int", "null", "string" ], [ "int", "string", "null" ], will all be parsed as
    // StructType { member0 -> IntegerType, member1 -> StringType }.
    Validate.isInstanceOf(StructType.class, dataType, "Expected StructType, got: " + dataType.getClass().getName());
    Validate.isInstanceOf(Row.class, o, "Expected Row, got: " + o.getClass().getName());
    Row row = (Row) o;

    StructType structType = (StructType) dataType;
    StructField[] structFields = structType.fields();
    int structFieldIndex = 0;
    for (Schema type: types) {
      Validate.isTrue(type.getType() != Schema.Type.NULL);

      Object unionField = row.get(structFieldIndex);
      if (unionField != null) {
        return convertInternal(unionField, structFields[structFieldIndex].dataType(), type);
      }
      structFieldIndex++;
    }

    throw new IllegalArgumentException("At least one field of complex union must be non-null: " + types);
  }

  private static Object convertInternal(Object o, DataType dataType, Schema schema) {
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

  static LogicalType validateLogicalType(Schema schema, LogicalType... expectedTypes) {
    return validateLogicalType(schema, true, expectedTypes);
  }

  static LogicalType validateLogicalType(Schema schema, boolean needLogicalType, LogicalType... expectedTypes) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == null) {
      if (needLogicalType) {
        throw new IllegalArgumentException("Expected Avro logical type to be present, got schema: " + schema);
      } else {
        return null;
      }
    }

    for (LogicalType expectedType: expectedTypes) {
      if (logicalType.equals(expectedType)) {
        return expectedType;
      }
    }

    throw new IllegalArgumentException(
        "Expected Avro logical type to be one of: " + Arrays.toString(expectedTypes) + ", got: " + logicalType);
  }
}
