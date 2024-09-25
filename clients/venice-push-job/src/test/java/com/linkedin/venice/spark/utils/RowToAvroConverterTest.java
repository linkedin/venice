package com.linkedin.venice.spark.utils;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.Time;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType$;
import org.testng.annotations.Test;
import scala.collection.JavaConverters;


public class RowToAvroConverterTest {
  private static final StructType COMPLEX_SUB_SCHEMA = DataTypes.createStructType(
      new StructField[] { new StructField("int", IntegerType, false, Metadata.empty()),
          new StructField("string", StringType, false, Metadata.empty()) });

  private static final StructType UNION_STRUCT_STRING_INT = DataTypes.createStructType(
      new StructField[] { new StructField("member0", StringType, true, Metadata.empty()),
          new StructField("member1", IntegerType, true, Metadata.empty()) });

  private static final StructType UNION_STRUCT_DOUBLE_FLOAT_STRING = DataTypes.createStructType(
      new StructField[] { new StructField("member0", DoubleType, true, Metadata.empty()),
          new StructField("member1", FloatType, true, Metadata.empty()),
          new StructField("member2", StringType, true, Metadata.empty()) });

  private static final StructType SPARK_STRUCT_SCHEMA = new StructType(
      new StructField[] { new StructField("byteArr", BinaryType, false, Metadata.empty()),
          new StructField("byteBuffer", BinaryType, false, Metadata.empty()),
          new StructField("decimalBytes", DataTypes.createDecimalType(3, 2), false, Metadata.empty()),
          new StructField("booleanTrue", BooleanType, false, Metadata.empty()),
          new StructField("booleanFalse", BooleanType, false, Metadata.empty()),
          new StructField("float", FloatType, false, Metadata.empty()),
          new StructField("double", DoubleType, false, Metadata.empty()),
          new StructField("string", StringType, false, Metadata.empty()),
          new StructField("byteArrFixed", BinaryType, false, Metadata.empty()),
          new StructField("byteBufferFixed", BinaryType, false, Metadata.empty()),
          new StructField("decimalFixed", DataTypes.createDecimalType(3, 2), false, Metadata.empty()),
          new StructField("enumType", StringType, false, Metadata.empty()),
          new StructField("int", IntegerType, false, Metadata.empty()),
          new StructField("date", DateType, false, Metadata.empty()),
          new StructField("dateLocal", DateType, false, Metadata.empty()),
          new StructField("byte", ByteType, false, Metadata.empty()),
          new StructField("short", ShortType, false, Metadata.empty()),
          new StructField("yearMonthInterval", DataTypes.createYearMonthIntervalType(), false, Metadata.empty()),
          new StructField("long", LongType, false, Metadata.empty()),
          new StructField("instantMicros", TimestampType, false, Metadata.empty()),
          new StructField("instantMillis", TimestampType, false, Metadata.empty()),
          new StructField("timestampMicros", TimestampType, false, Metadata.empty()),
          new StructField("timestampMillis", TimestampType, false, Metadata.empty()),
          new StructField("timestampNoLogical", TimestampType, false, Metadata.empty()),
          new StructField("localTimestampMicros", TimestampNTZType$.MODULE$, false, Metadata.empty()),
          new StructField("localTimestampMillis", TimestampNTZType$.MODULE$, false, Metadata.empty()),
          new StructField("localTimestampNoLogical", TimestampNTZType$.MODULE$, false, Metadata.empty()),
          new StructField("dayTimeInterval", DataTypes.createDayTimeIntervalType(), false, Metadata.empty()),
          new StructField("arrayIntList", DataTypes.createArrayType(IntegerType), false, Metadata.empty()),
          new StructField("arrayIntSeq", DataTypes.createArrayType(IntegerType), false, Metadata.empty()),
          new StructField("arrayComplex", DataTypes.createArrayType(COMPLEX_SUB_SCHEMA), false, Metadata.empty()),
          new StructField("mapIntJavaMap", DataTypes.createMapType(StringType, IntegerType), false, Metadata.empty()),
          new StructField("mapIntScalaMap", DataTypes.createMapType(StringType, IntegerType), false, Metadata.empty()),
          new StructField(
              "mapComplex",
              DataTypes.createMapType(StringType, COMPLEX_SUB_SCHEMA),
              false,
              Metadata.empty()),
          new StructField("nullableUnion", IntegerType, true, Metadata.empty()),
          new StructField("nullableUnion2", IntegerType, true, Metadata.empty()),
          new StructField("singleElementUnion", IntegerType, false, Metadata.empty()),
          new StructField("intLongUnion", LongType, false, Metadata.empty()),
          new StructField("longIntUnion", LongType, false, Metadata.empty()),
          new StructField("floatDoubleUnion", DoubleType, false, Metadata.empty()),
          new StructField("doubleFloatUnion", DoubleType, false, Metadata.empty()),
          new StructField("complexNonNullableUnion", UNION_STRUCT_DOUBLE_FLOAT_STRING, false, Metadata.empty()),
          new StructField("complexNullableUnion1", UNION_STRUCT_STRING_INT, true, Metadata.empty()),
          new StructField("complexNullableUnion2", UNION_STRUCT_STRING_INT, true, Metadata.empty()),
          new StructField("complexNullableUnion3", UNION_STRUCT_STRING_INT, true, Metadata.empty()), });

  private static final Schema DECIMAL_TYPE = LogicalTypes.decimal(3, 2).addToSchema(Schema.create(Schema.Type.BYTES));
  private static final Schema DECIMAL_FIXED_TYPE =
      LogicalTypes.decimal(3, 2).addToSchema(Schema.createFixed("decimalFixed", null, null, 3));
  private static final Schema FIXED_TYPE_3 = Schema.createFixed("decimalFixed", null, null, 3);
  private static final String STRING_VALUE = "PAX TIBI MARCE EVANGELISTA MEVS";
  private static final String STRING_VALUE_2 =
      "It’s temples and palaces did seem like fabrics of enchantment piled to heaven";
  private static final String STRING_VALUE_3 = "Like eating an entire box of chocolate liqueurs in one go";
  private static final Schema DATE_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
  private static final Schema TIMESTAMP_MICROS =
      LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMP_MILLIS =
      LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
  private static final long TEST_EPOCH_MILLIS = 1718860000000L;
  private static final Instant TEST_EPOCH_INSTANT = Instant.ofEpochMilli(TEST_EPOCH_MILLIS);
  private static final Schema LOCAL_TIMESTAMP_MICROS =
      LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema LOCAL_TIMESTAMP_MILLIS =
      LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
  private static final LocalDateTime TEST_LOCAL_DATE_TIME =
      LocalDateTime.ofEpochSecond(TEST_EPOCH_MILLIS / 1000, 0, ZoneOffset.of("+02:00"));
  // 2 hour offset to account for the local timezone
  private static final long TEST_LOCAL_TIMESTAMP_MILLIS = TEST_EPOCH_MILLIS + 2 * Time.MS_PER_HOUR;

  private static final Schema COMPLEX_SUB_SCHEMA_AVRO =
      SchemaBuilder.record("arrayComplex").fields().requiredInt("int").requiredString("string").endRecord();

  private static final Schema AVRO_SCHEMA = SchemaBuilder.record("test")
      .fields()
      .name("byteArr")
      .type()
      .bytesType()
      .noDefault()
      .name("byteBuffer")
      .type()
      .bytesType()
      .noDefault()
      .name("decimalBytes")
      .type(DECIMAL_TYPE)
      .noDefault()
      .name("booleanTrue")
      .type()
      .booleanType()
      .noDefault()
      .name("booleanFalse")
      .type()
      .booleanType()
      .noDefault()
      .name("float")
      .type()
      .floatType()
      .noDefault()
      .name("double")
      .type()
      .doubleType()
      .noDefault()
      .name("string")
      .type()
      .stringType()
      .noDefault()
      .name("byteArrFixed")
      .type(FIXED_TYPE_3)
      .noDefault()
      .name("byteBufferFixed")
      .type(FIXED_TYPE_3)
      .noDefault()
      .name("decimalFixed")
      .type(DECIMAL_FIXED_TYPE)
      .noDefault()
      .name("enumType")
      .type()
      .enumeration("enumType")
      .symbols("A", "B", "C")
      .noDefault()
      .name("int")
      .type()
      .intType()
      .noDefault()
      .name("date")
      .type(DATE_TYPE)
      .noDefault()
      .name("dateLocal")
      .type(DATE_TYPE)
      .noDefault()
      .name("byte")
      .type()
      .intType()
      .noDefault()
      .name("short")
      .type()
      .intType()
      .noDefault()
      .name("yearMonthInterval")
      .type()
      .intType()
      .noDefault()
      .name("long")
      .type()
      .longType()
      .noDefault()
      .name("instantMicros")
      .type(TIMESTAMP_MICROS)
      .noDefault()
      .name("instantMillis")
      .type(TIMESTAMP_MILLIS)
      .noDefault()
      .name("timestampMicros")
      .type(TIMESTAMP_MICROS)
      .noDefault()
      .name("timestampMillis")
      .type(TIMESTAMP_MILLIS)
      .noDefault()
      .name("timestampNoLogical")
      .type()
      .longType()
      .noDefault()
      .name("localTimestampMicros")
      .type(LOCAL_TIMESTAMP_MICROS)
      .noDefault()
      .name("localTimestampMillis")
      .type(LOCAL_TIMESTAMP_MILLIS)
      .noDefault()
      .name("localTimestampNoLogical")
      .type()
      .longType()
      .noDefault()
      .name("dayTimeInterval")
      .type()
      .longType()
      .noDefault()
      .name("arrayIntList")
      .type()
      .array()
      .items()
      .intType()
      .noDefault()
      .name("arrayIntSeq")
      .type()
      .array()
      .items()
      .intType()
      .noDefault()
      .name("arrayComplex")
      .type()
      .array()
      .items(COMPLEX_SUB_SCHEMA_AVRO)
      .noDefault()
      .name("mapIntJavaMap")
      .type()
      .map()
      .values()
      .intType()
      .noDefault()
      .name("mapIntScalaMap")
      .type()
      .map()
      .values()
      .intType()
      .noDefault()
      .name("mapComplex")
      .type()
      .map()
      .values(COMPLEX_SUB_SCHEMA_AVRO)
      .noDefault()
      .name("nullableUnion")
      .type()
      .unionOf()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .name("nullableUnion2")
      .type()
      .unionOf()
      .intType()
      .and()
      .nullType()
      .endUnion()
      .noDefault()
      .name("singleElementUnion")
      .type()
      .unionOf()
      .intType()
      .endUnion()
      .noDefault()
      .name("intLongUnion")
      .type()
      .unionOf()
      .intType()
      .and()
      .longType()
      .endUnion()
      .noDefault()
      .name("longIntUnion")
      .type()
      .unionOf()
      .longType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .name("floatDoubleUnion")
      .type()
      .unionOf()
      .floatType()
      .and()
      .doubleType()
      .endUnion()
      .noDefault()
      .name("doubleFloatUnion")
      .type()
      .unionOf()
      .doubleType()
      .and()
      .floatType()
      .endUnion()
      .noDefault()
      .name("complexNonNullableUnion")
      .type()
      .unionOf()
      .doubleType()
      .and()
      .floatType()
      .and()
      .stringType()
      .endUnion()
      .noDefault()
      .name("complexNullableUnion1")
      .type()
      .unionOf()
      .nullType()
      .and()
      .stringType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .name("complexNullableUnion2")
      .type()
      .unionOf()
      .stringType()
      .and()
      .nullType()
      .and()
      .intType()
      .endUnion()
      .noDefault()
      .name("complexNullableUnion3")
      .type()
      .unionOf()
      .stringType()
      .and()
      .intType()
      .and()
      .nullType()
      .endUnion()
      .noDefault()
      .endRecord();

  private static final Row SPARK_ROW = new GenericRowWithSchema(
      new Object[] { new byte[] { 0x01, 0x02, 0x03 }, // byteArr
          ByteBuffer.wrap(new byte[] { 0x04, 0x05, 0x06 }), // byteBuffer
          new BigDecimal("0.456").setScale(2, RoundingMode.HALF_UP), // decimalBytes
          true, // booleanTrue
          false, // booleanFalse
          0.5f, // float
          0.7, // double
          STRING_VALUE, // string
          new byte[] { 0x01, 0x02, 0x03 }, // byteArrFixed
          ByteBuffer.wrap(new byte[] { 0x04, 0x05, 0x06 }), // byteBufferFixed
          new BigDecimal("0.456").setScale(2, RoundingMode.HALF_UP), // decimalFixed
          "A", // enumType
          100, // int
          Date.valueOf(LocalDate.of(2024, 6, 18)), // date
          LocalDate.of(2024, 6, 18), // dateLocal
          (byte) 100, // byte
          (short) 100, // short
          Period.ofMonths(5), // yearMonthInterval
          100L, // long
          TEST_EPOCH_INSTANT, // instantMicros
          TEST_EPOCH_INSTANT, // instantMillis
          Timestamp.from(TEST_EPOCH_INSTANT), // timestampMicros
          Timestamp.from(TEST_EPOCH_INSTANT), // timestampMillis
          TEST_EPOCH_INSTANT, // timestampNoLogical
          TEST_LOCAL_DATE_TIME, // localTimestampMicros
          TEST_LOCAL_DATE_TIME, // localTimestampMillis
          TEST_LOCAL_DATE_TIME, // localTimestampNoLogical
          Duration.ofSeconds(100), // dayTimeInterval
          Arrays.asList(1, 2, 3), // arrayIntList
          JavaConverters.asScalaBuffer(Arrays.asList(1, 2, 3)).toList(), // arrayIntSeq
          JavaConverters.asScalaBuffer(
              Arrays.asList(
                  new GenericRowWithSchema(new Object[] { 10, STRING_VALUE_2 }, COMPLEX_SUB_SCHEMA),
                  new GenericRowWithSchema(new Object[] { 20, STRING_VALUE_3 }, COMPLEX_SUB_SCHEMA)))
              .toList(), // arrayComplex
          new HashMap<String, Integer>() {
            {
              put("key1", 10);
              put("key2", 20);
            }
          }, // mapIntJavaMap
          JavaConverters.mapAsScalaMap(new HashMap<String, Integer>() {
            {
              put("key1", 10);
              put("key2", 20);
            }
          }), // mapIntScalaMap
          new HashMap<String, Row>() {
            {
              put("key1", new GenericRowWithSchema(new Object[] { 10, STRING_VALUE_2 }, COMPLEX_SUB_SCHEMA));
              put("key2", new GenericRowWithSchema(new Object[] { 20, STRING_VALUE_3 }, COMPLEX_SUB_SCHEMA));
            }
          }, // mapComplex
          10, // nullableUnion
          null, // nullableUnion2
          10, // singleElementUnion
          10L, // intLongUnion
          10L, // longIntUnion
          0.5, // floatDoubleUnion
          0.5, // doubleFloatUnion
          new GenericRowWithSchema(new Object[] { null, 0.5f, null }, UNION_STRUCT_DOUBLE_FLOAT_STRING), // complexNonNullableUnion
          new GenericRowWithSchema(new Object[] { null, 10 }, UNION_STRUCT_STRING_INT), // complexNullableUnion1
          new GenericRowWithSchema(new Object[] { STRING_VALUE, null }, UNION_STRUCT_STRING_INT), // complexNullableUnion2
          null, // complexNullableUnion3
      },
      SPARK_STRUCT_SCHEMA);

  @Test
  public void testConvertToRecord() {
    GenericRecord record = RowToAvroConverter.convertToRecord(SPARK_ROW, SPARK_STRUCT_SCHEMA, AVRO_SCHEMA);
    assertEquals(record.get("byteArr"), ByteBuffer.wrap(new byte[] { 0x01, 0x02, 0x03 }));
    assertEquals(record.get("byteBuffer"), ByteBuffer.wrap(new byte[] { 0x04, 0x05, 0x06 }));
    assertEquals(record.get("decimalBytes"), ByteBuffer.wrap(new byte[] { 46 }));
    assertEquals(record.get("booleanTrue"), true);
    assertEquals(record.get("booleanFalse"), false);
    assertEquals(record.get("float"), 0.5f);
    assertEquals(record.get("double"), 0.7);
    assertEquals(record.get("string"), STRING_VALUE);
    assertEquals(
        record.get("byteArrFixed"),
        AvroCompatibilityHelper.newFixed(FIXED_TYPE_3, new byte[] { 0x01, 0x02, 0x03 }));
    assertEquals(
        record.get("byteBufferFixed"),
        AvroCompatibilityHelper.newFixed(FIXED_TYPE_3, new byte[] { 0x04, 0x05, 0x06 }));
    assertEquals(record.get("decimalFixed"), AvroCompatibilityHelper.newFixed(FIXED_TYPE_3, new byte[] { 0, 0, 46 }));
    assertEquals(
        record.get("enumType"),
        AvroCompatibilityHelper.newEnumSymbol(AVRO_SCHEMA.getField("enumType").schema(), "A"));
    assertEquals(record.get("int"), 100);
    assertEquals(record.get("date"), (int) LocalDate.of(2024, 6, 18).toEpochDay());
    assertEquals(record.get("dateLocal"), (int) LocalDate.of(2024, 6, 18).toEpochDay());
    assertEquals(record.get("byte"), 100);
    assertEquals(record.get("short"), 100);
    assertEquals(record.get("yearMonthInterval"), 5);
    assertEquals(record.get("long"), 100L);
    assertEquals(record.get("instantMicros"), TEST_EPOCH_MILLIS * 1000);
    assertEquals(record.get("instantMillis"), TEST_EPOCH_MILLIS);
    assertEquals(record.get("timestampMicros"), TEST_EPOCH_MILLIS * 1000);
    assertEquals(record.get("timestampMillis"), TEST_EPOCH_MILLIS);
    assertEquals(record.get("timestampNoLogical"), TEST_EPOCH_MILLIS);
    assertEquals(record.get("localTimestampMicros"), TEST_LOCAL_TIMESTAMP_MILLIS * 1000);
    assertEquals(record.get("localTimestampMillis"), TEST_LOCAL_TIMESTAMP_MILLIS);
    assertEquals(record.get("localTimestampNoLogical"), TEST_LOCAL_TIMESTAMP_MILLIS);
    assertEquals(record.get("dayTimeInterval"), 100L * 1000 * 1000);
    assertEquals(record.get("arrayIntList"), Arrays.asList(1, 2, 3));
    assertEquals(record.get("arrayIntSeq"), Arrays.asList(1, 2, 3));

    GenericRecord complex_record_1 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_1.put("int", 10);
    complex_record_1.put("string", STRING_VALUE_2);

    GenericRecord complex_record_2 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_2.put("int", 20);
    complex_record_2.put("string", STRING_VALUE_3);

    assertEquals(record.get("arrayComplex"), Arrays.asList(complex_record_1, complex_record_2));

    Map<String, Integer> expectedIntMap = new HashMap<String, Integer>() {
      {
        put("key1", 10);
        put("key2", 20);
      }
    };
    assertEquals(record.get("mapIntJavaMap"), expectedIntMap);
    assertEquals(record.get("mapIntScalaMap"), expectedIntMap);

    Map<String, GenericRecord> expectedComplexMap = new HashMap<String, GenericRecord>() {
      {
        put("key1", complex_record_1);
        put("key2", complex_record_2);
      }
    };
    assertEquals(record.get("mapComplex"), expectedComplexMap);

    assertEquals(record.get("nullableUnion"), 10);

    assertNull(record.get("nullableUnion2"));

    assertEquals(record.get("singleElementUnion"), 10);

    assertEquals(record.get("intLongUnion"), 10L);

    assertEquals(record.get("longIntUnion"), 10L);

    assertEquals(record.get("floatDoubleUnion"), 0.5);

    assertEquals(record.get("doubleFloatUnion"), 0.5);

    Object complexNonNullableUnion = record.get("complexNonNullableUnion");
    assertTrue(complexNonNullableUnion instanceof Float);
    assertEquals((Float) complexNonNullableUnion, 0.5f, 0.001f);

    Object complexNullableUnion1 = record.get("complexNullableUnion1");
    assertTrue(complexNullableUnion1 instanceof Integer);
    assertEquals(((Integer) complexNullableUnion1).intValue(), 10);

    Object complexNullableUnion2 = record.get("complexNullableUnion2");
    assertTrue(complexNullableUnion2 instanceof CharSequence);
    assertEquals(complexNullableUnion2, STRING_VALUE);

    assertNull(record.get("complexNullableUnion3"));
  }

  @Test
  public void testConvertToBoolean() {
    Boolean trueObj = RowToAvroConverter.convertToBoolean(true, BooleanType);
    assertNotNull(trueObj);
    assertTrue(trueObj);

    Boolean falseObj = RowToAvroConverter.convertToBoolean(false, BooleanType);
    assertNotNull(falseObj);
    assertFalse(falseObj);

    // Type must be BooleanType
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToBoolean(true, ByteType));

    // Data must be Boolean
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToBoolean(10, BooleanType));
  }

  @Test
  public void testConvertToInt() {
    Integer integer =
        RowToAvroConverter.convertToInt(SPARK_ROW.getAs("int"), IntegerType, AVRO_SCHEMA.getField("int").schema());
    assertNotNull(integer);
    assertEquals(integer.intValue(), 100);

    Integer date =
        RowToAvroConverter.convertToInt(SPARK_ROW.getAs("date"), DateType, AVRO_SCHEMA.getField("date").schema());
    assertNotNull(date);
    assertEquals(date.intValue(), (int) LocalDate.of(2024, 6, 18).toEpochDay());

    Integer dateLocal = RowToAvroConverter
        .convertToInt(SPARK_ROW.getAs("dateLocal"), DateType, AVRO_SCHEMA.getField("dateLocal").schema());
    assertNotNull(dateLocal);
    assertEquals(dateLocal.intValue(), (int) LocalDate.of(2024, 6, 18).toEpochDay());

    Integer byteInt =
        RowToAvroConverter.convertToInt(SPARK_ROW.getAs("byte"), ByteType, AVRO_SCHEMA.getField("byte").schema());
    assertNotNull(byteInt);
    assertEquals(byteInt.intValue(), 100);

    Integer shortInt =
        RowToAvroConverter.convertToInt(SPARK_ROW.getAs("short"), ShortType, AVRO_SCHEMA.getField("short").schema());
    assertNotNull(shortInt);
    assertEquals(shortInt.intValue(), 100);

    Integer yearMonthInterval = RowToAvroConverter.convertToInt(
        SPARK_ROW.getAs("yearMonthInterval"),
        DataTypes.createYearMonthIntervalType(),
        AVRO_SCHEMA.getField("yearMonthInterval").schema());
    assertNotNull(yearMonthInterval);
    assertEquals(yearMonthInterval.intValue(), 5);

    // Type must be IntegerType, ByteType, ShortType, DateType or YearMonthIntervalType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(0.5f, StringType, AVRO_SCHEMA.getField("int").schema()));

    // When using IntegerType, data must be an Integer
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(10.0, IntegerType, AVRO_SCHEMA.getField("int").schema()));

    // When using ByteType, data must be a Byte
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(10.0, ByteType, AVRO_SCHEMA.getField("byte").schema()));

    // When using ShortType, data must be a Short
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(10.0, ShortType, AVRO_SCHEMA.getField("short").schema()));

    // When using DateType, data must be a java.time.LocalDate or java.sql.Date
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(10.0, DateType, AVRO_SCHEMA.getField("date").schema()));
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(10.0, DateType, AVRO_SCHEMA.getField("dateLocal").schema()));

    // When using DateType, the Avro schema must have a logical type of Date
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToInt(LocalDate.of(2024, 6, 18), DateType, AVRO_SCHEMA.getField("int").schema()));

    // When using YearMonthIntervalType, data must be a Period
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToInt(
            10.0,
            DataTypes.createYearMonthIntervalType(),
            AVRO_SCHEMA.getField("yearMonthInterval").schema()));
  }

  @Test
  public void testConvertToLong() {
    Long longType =
        RowToAvroConverter.convertToLong(SPARK_ROW.getAs("long"), LongType, AVRO_SCHEMA.getField("long").schema());
    assertNotNull(longType);
    assertEquals(longType.intValue(), 100);

    Long instantMicros = RowToAvroConverter
        .convertToLong(SPARK_ROW.getAs("instantMicros"), TimestampType, AVRO_SCHEMA.getField("instantMicros").schema());
    assertNotNull(instantMicros);
    assertEquals(instantMicros.longValue(), TEST_EPOCH_MILLIS * 1000);

    Long instantMillis = RowToAvroConverter
        .convertToLong(SPARK_ROW.getAs("instantMillis"), TimestampType, AVRO_SCHEMA.getField("instantMillis").schema());
    assertNotNull(instantMillis);
    assertEquals(instantMillis.longValue(), TEST_EPOCH_MILLIS);

    Long timestampMicros = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("timestampMicros"),
        TimestampType,
        AVRO_SCHEMA.getField("timestampMicros").schema());
    assertNotNull(timestampMicros);
    assertEquals(timestampMicros.longValue(), TEST_EPOCH_MILLIS * 1000);

    Long timestampMillis = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("timestampMillis"),
        TimestampType,
        AVRO_SCHEMA.getField("timestampMillis").schema());
    assertNotNull(timestampMillis);
    assertEquals(timestampMillis.longValue(), TEST_EPOCH_MILLIS);

    // When using TimestampType, and there is no logical type on the Avro schema, convert to millis by default
    Long timestampNoLogical = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("timestampNoLogical"),
        TimestampType,
        AVRO_SCHEMA.getField("timestampNoLogical").schema());
    assertNotNull(timestampNoLogical);
    assertEquals(timestampNoLogical.longValue(), TEST_EPOCH_MILLIS);

    Long localTimestampMicros = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("localTimestampMicros"),
        TimestampNTZType$.MODULE$,
        AVRO_SCHEMA.getField("localTimestampMicros").schema());
    assertNotNull(localTimestampMicros);
    assertEquals(localTimestampMicros.longValue(), TEST_LOCAL_TIMESTAMP_MILLIS * 1000);

    Long localTimestampMillis = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("localTimestampMillis"),
        TimestampNTZType$.MODULE$,
        AVRO_SCHEMA.getField("localTimestampMillis").schema());
    assertNotNull(localTimestampMillis);
    assertEquals(localTimestampMillis.longValue(), TEST_LOCAL_TIMESTAMP_MILLIS);

    // When using TimestampNTZType, and there is no logical type on the Avro schema, convert to millis by default
    Long localTimestampNoLogical = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("localTimestampNoLogical"),
        TimestampNTZType$.MODULE$,
        AVRO_SCHEMA.getField("localTimestampNoLogical").schema());
    assertNotNull(localTimestampNoLogical);
    assertEquals(localTimestampNoLogical.longValue(), TEST_LOCAL_TIMESTAMP_MILLIS);

    Long dayTimeInterval = RowToAvroConverter.convertToLong(
        SPARK_ROW.getAs("dayTimeInterval"),
        DataTypes.createDayTimeIntervalType(),
        AVRO_SCHEMA.getField("dayTimeInterval").schema());
    assertNotNull(dayTimeInterval);
    assertEquals(dayTimeInterval.longValue(), 100L * 1000 * 1000);

    // Type must be LongType, TimestampType, TimestampNTZType or DayTimeIntervalType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(0.5f, StringType, AVRO_SCHEMA.getField("long").schema()));

    // When using LongType, data must be a Long
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(10.0, LongType, AVRO_SCHEMA.getField("long").schema()));

    // When using TimestampType, data must be a java.time.Instant or java.sql.Timestamp
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(10.0, TimestampType, AVRO_SCHEMA.getField("instantMicros").schema()));
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(10.0, TimestampType, AVRO_SCHEMA.getField("instantMillis").schema()));
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(10.0, TimestampType, AVRO_SCHEMA.getField("timestampMicros").schema()));
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(10.0, TimestampType, AVRO_SCHEMA.getField("timestampMillis").schema()));

    // When using TimestampNTZType, data must be a java.time.LocalDateTime
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToLong(10.0, TimestampNTZType$.MODULE$, AVRO_SCHEMA.getField("localTimestampNoLogical").schema()));

    // When using DayTimeIntervalType, data must be a Duration
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToLong(
            10.0,
            DataTypes.createDayTimeIntervalType(),
            AVRO_SCHEMA.getField("dayTimeInterval").schema()));
  }

  @Test
  public void testConvertToFloat() {
    Float floatObj = RowToAvroConverter.convertToFloat(0.5f, FloatType);
    assertNotNull(floatObj);
    assertEquals(floatObj, 0.5f, 0.0001f);

    // Type must be FloatType
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToFloat(0.5f, ByteType));

    // Data must be Float
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToFloat(10, FloatType));
  }

  @Test
  public void testConvertToDouble() {
    Double doubleObj = RowToAvroConverter.convertToDouble(0.7, DoubleType);
    assertNotNull(doubleObj);
    assertEquals(doubleObj, 0.7, 0.0001);

    // Type must be DoubleType
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToDouble(0.7, ByteType));

    // Data must be Double
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToDouble(true, DoubleType));
  }

  @Test
  public void testConvertToString() {
    CharSequence strObj = RowToAvroConverter.convertToString(STRING_VALUE, StringType);
    assertNotNull(strObj);
    assertEquals(strObj, STRING_VALUE);

    // Type must be StringType
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToString(STRING_VALUE, ByteType));

    // Data must be String
    assertThrows(IllegalArgumentException.class, () -> RowToAvroConverter.convertToString(100, StringType));
  }

  @Test
  public void testConvertToBytes() {
    ByteBuffer byteArrObj = RowToAvroConverter
        .convertToBytes(SPARK_ROW.getAs("byteArr"), BinaryType, AVRO_SCHEMA.getField("byteArr").schema());
    assertNotNull(byteArrObj);
    assertEquals(byteArrObj, ByteBuffer.wrap(new byte[] { 0x01, 0x02, 0x03 }));

    ByteBuffer byteBufferObj = RowToAvroConverter
        .convertToBytes(SPARK_ROW.getAs("byteBuffer"), BinaryType, AVRO_SCHEMA.getField("byteBuffer").schema());
    assertNotNull(byteBufferObj);
    assertEquals(byteBufferObj, ByteBuffer.wrap(new byte[] { 0x04, 0x05, 0x06 }));

    ByteBuffer decimalObj = RowToAvroConverter.convertToBytes(
        SPARK_ROW.getAs("decimalBytes"),
        DataTypes.createDecimalType(3, 2),
        AVRO_SCHEMA.getField("decimalBytes").schema());
    assertNotNull(decimalObj);
    assertEquals(decimalObj, ByteBuffer.wrap(new byte[] { 46 }));

    // The scale of the actual BigDecimal object shouldn't matter
    ByteBuffer decimalObj2 = RowToAvroConverter.convertToBytes(
        new BigDecimal("0.456").setScale(1, RoundingMode.HALF_UP),
        DataTypes.createDecimalType(3, 2),
        DECIMAL_TYPE);
    assertNotNull(decimalObj2);
    assertEquals(decimalObj2, ByteBuffer.wrap(new byte[] { 50 }));

    // Type must be BinaryType or DecimalType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToBytes(SPARK_ROW.getAs("byteArr"), ByteType, AVRO_SCHEMA.getField("byteArr").schema()));

    // Data must be byte[], ByteBuffer or BigDecimal
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToBytes(SPARK_ROW.getAs("booleanTrue"), BinaryType, AVRO_SCHEMA.getField("booleanTrue").schema()));

    // Logical type scale must match the Spark type scale
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToBytes(
            new BigDecimal("0.456").setScale(2, RoundingMode.HALF_UP),
            DataTypes.createDecimalType(3, 2),
            LogicalTypes.decimal(3, 3).addToSchema(Schema.create(Schema.Type.BYTES))));

    // Logical type precision must match the Spark type precision
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToBytes(
            new BigDecimal("0.456").setScale(4, RoundingMode.HALF_UP),
            DataTypes.createDecimalType(4, 3),
            LogicalTypes.decimal(3, 3).addToSchema(Schema.create(Schema.Type.BYTES))));
  }

  @Test
  public void testConvertToFixed() {
    GenericFixed byteArrObj = RowToAvroConverter
        .convertToFixed(SPARK_ROW.getAs("byteArrFixed"), BinaryType, AVRO_SCHEMA.getField("byteArrFixed").schema());
    assertNotNull(byteArrObj);
    assertNotNull(byteArrObj.bytes());
    assertEquals(byteArrObj.bytes().length, 3);
    assertEquals(byteArrObj.bytes(), new byte[] { 0x01, 0x02, 0x03 });

    GenericFixed byteBufferObj = RowToAvroConverter.convertToFixed(
        SPARK_ROW.getAs("byteBufferFixed"),
        BinaryType,
        AVRO_SCHEMA.getField("byteBufferFixed").schema());
    assertNotNull(byteBufferObj);
    assertNotNull(byteBufferObj.bytes());
    assertEquals(byteBufferObj.bytes().length, 3);
    assertEquals(byteBufferObj.bytes(), new byte[] { 0x04, 0x05, 0x06 });

    GenericFixed decimalObj = RowToAvroConverter.convertToFixed(
        SPARK_ROW.getAs("decimalFixed"),
        DataTypes.createDecimalType(3, 2),
        AVRO_SCHEMA.getField("decimalFixed").schema());
    assertNotNull(decimalObj);
    assertNotNull(decimalObj.bytes());
    assertEquals(decimalObj.bytes().length, 3);
    assertEquals(decimalObj.bytes(), new byte[] { 0, 0, 46 });

    // The scale of the actual BigDecimal object shouldn't matter
    GenericFixed decimalObj2 = RowToAvroConverter.convertToFixed(
        new BigDecimal("0.456").setScale(1, RoundingMode.HALF_UP),
        DataTypes.createDecimalType(3, 2),
        DECIMAL_FIXED_TYPE);
    assertNotNull(decimalObj2);
    assertNotNull(decimalObj2.bytes());
    assertEquals(decimalObj2.bytes().length, 3);
    assertEquals(decimalObj2.bytes(), new byte[] { 0, 0, 50 });

    // The byte array must have the correct length
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToFixed(new byte[] { 0x00, 0x01 }, BinaryType, AVRO_SCHEMA.getField("byteArrFixed").schema()));

    // The byte buffer must have the correct length
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToFixed(
            ByteBuffer.wrap(new byte[] { 0x00, 0x01 }),
            BinaryType,
            AVRO_SCHEMA.getField("byteBufferFixed").schema()));

    // Type must be BinaryType or DecimalType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToFixed(SPARK_ROW.getAs("byteArrFixed"), ByteType, AVRO_SCHEMA.getField("byteArrFixed").schema()));

    // Data must be byte[], ByteBuffer or BigDecimal
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToFixed(SPARK_ROW.getAs("booleanTrue"), BinaryType, AVRO_SCHEMA.getField("booleanTrue").schema()));

    // Logical type scale must match the Spark type scale
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToFixed(
            new BigDecimal("0.456").setScale(2, RoundingMode.HALF_UP),
            DataTypes.createDecimalType(3, 2),
            LogicalTypes.decimal(3, 3).addToSchema(Schema.createFixed("decimalFixed", null, null, 3))));

    // Logical type precision must match the Spark type precision
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToFixed(
            new BigDecimal("0.456").setScale(4, RoundingMode.HALF_UP),
            DataTypes.createDecimalType(4, 3),
            LogicalTypes.decimal(3, 3).addToSchema(Schema.createFixed("decimalFixed", null, null, 3))));
  }

  @Test
  public void testConvertToEnum() {
    GenericEnumSymbol enumObj = RowToAvroConverter
        .convertToEnum(SPARK_ROW.getAs("enumType"), StringType, AVRO_SCHEMA.getField("enumType").schema());
    assertNotNull(enumObj);
    assertEquals(enumObj, AvroCompatibilityHelper.newEnumSymbol(AVRO_SCHEMA.getField("enumType").schema(), "A"));

    // String value must be a valid symbol
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToEnum("D", StringType, AVRO_SCHEMA.getField("enumType").schema()));

    // Type must be StringType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToEnum(SPARK_ROW.getAs("enumType"), ByteType, AVRO_SCHEMA.getField("enumType").schema()));

    // Data must be String
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter
            .convertToEnum(SPARK_ROW.getAs("booleanTrue"), StringType, AVRO_SCHEMA.getField("enumType").schema()));
  }

  @Test
  public void testConvertToArray() {
    Schema arrayIntSchema = SchemaBuilder.array().items().intType();
    List<Object> arrayIntList = RowToAvroConverter
        .convertToArray(SPARK_ROW.getAs("arrayIntList"), DataTypes.createArrayType(IntegerType), arrayIntSchema);
    assertNotNull(arrayIntList);
    assertEquals(arrayIntList, Arrays.asList(1, 2, 3));

    List<Object> arrayIntSeq = RowToAvroConverter
        .convertToArray(SPARK_ROW.getAs("arrayIntSeq"), DataTypes.createArrayType(IntegerType), arrayIntSchema);
    assertNotNull(arrayIntSeq);
    assertEquals(arrayIntSeq, Arrays.asList(1, 2, 3));

    Schema arrayComplexSchema = SchemaBuilder.array().items(COMPLEX_SUB_SCHEMA_AVRO);
    List<Object> arrayComplex = RowToAvroConverter.convertToArray(
        SPARK_ROW.getAs("arrayComplex"),
        DataTypes.createArrayType(COMPLEX_SUB_SCHEMA),
        arrayComplexSchema);
    assertNotNull(arrayComplex);

    GenericRecord complex_record_1 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_1.put("int", 10);
    complex_record_1.put("string", STRING_VALUE_2);

    GenericRecord complex_record_2 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_2.put("int", 20);
    complex_record_2.put("string", STRING_VALUE_3);

    assertEquals(arrayComplex, Arrays.asList(complex_record_1, complex_record_2));

    // Type must be ArrayType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToArray(SPARK_ROW.getAs("arrayIntList"), ByteType, arrayIntSchema));

    // Data must be scala.collection.Seq or java.util.List
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToArray(100, DataTypes.createArrayType(IntegerType), arrayIntSchema));
  }

  @Test
  public void testConvertToMap() {
    Schema mapIntSchema = SchemaBuilder.map().values().intType();
    Map<String, Integer> expectedIntMap = new HashMap<String, Integer>() {
      {
        put("key1", 10);
        put("key2", 20);
      }
    };

    Map<CharSequence, Object> mapIntJavaMap = RowToAvroConverter
        .convertToMap(SPARK_ROW.getAs("mapIntJavaMap"), DataTypes.createMapType(StringType, IntegerType), mapIntSchema);
    assertNotNull(mapIntJavaMap);
    assertEquals(mapIntJavaMap, expectedIntMap);

    Map<CharSequence, Object> mapIntScalaMap = RowToAvroConverter.convertToMap(
        SPARK_ROW.getAs("mapIntScalaMap"),
        DataTypes.createMapType(StringType, IntegerType),
        mapIntSchema);
    assertNotNull(mapIntScalaMap);
    assertEquals(mapIntScalaMap, expectedIntMap);

    Schema mapComplexSchema = SchemaBuilder.map().values(COMPLEX_SUB_SCHEMA_AVRO);
    Map<CharSequence, Object> mapComplex = RowToAvroConverter.convertToMap(
        SPARK_ROW.getAs("mapComplex"),
        DataTypes.createMapType(StringType, COMPLEX_SUB_SCHEMA),
        mapComplexSchema);
    assertNotNull(mapComplex);

    GenericRecord complex_record_1 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_1.put("int", 10);
    complex_record_1.put("string", STRING_VALUE_2);

    GenericRecord complex_record_2 = new GenericData.Record(COMPLEX_SUB_SCHEMA_AVRO);
    complex_record_2.put("int", 20);
    complex_record_2.put("string", STRING_VALUE_3);

    Map<String, GenericRecord> expectedComplexMap = new HashMap<String, GenericRecord>() {
      {
        put("key1", complex_record_1);
        put("key2", complex_record_2);
      }
    };

    assertEquals(mapComplex, expectedComplexMap);

    // Maps with keys that are not String are not supported
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToMap(
            SPARK_ROW.getAs("mapIntJavaMap"),
            DataTypes.createMapType(ByteType, IntegerType),
            mapIntSchema));

    // Type must be MapType
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToMap(SPARK_ROW.getAs("mapIntJavaMap"), ByteType, mapIntSchema));

    // Data must be scala.collection.Map or java.util.Map
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToMap(100, DataTypes.createMapType(StringType, IntegerType), mapIntSchema));
  }

  @Test
  public void testConvertToUnion() {
    // null is allowed for nullable unions
    assertNull(RowToAvroConverter.convertToUnion(null, IntegerType, AVRO_SCHEMA.getField("nullableUnion").schema()));

    // null is not allowed for non-nullable unions
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToUnion(null, LongType, AVRO_SCHEMA.getField("longIntUnion").schema()));

    // Test union with only 1 branch
    Object singleElementUnion =
        RowToAvroConverter.convertToUnion(10, IntegerType, AVRO_SCHEMA.getField("singleElementUnion").schema());
    assertTrue(singleElementUnion instanceof Integer);
    assertEquals(((Integer) singleElementUnion).intValue(), 10);

    // Test union with two branches: null + something else
    assertNull(RowToAvroConverter.convertToUnion(null, IntegerType, AVRO_SCHEMA.getField("nullableUnion").schema()));
    Object nullableUnionValue =
        RowToAvroConverter.convertToUnion(10, IntegerType, AVRO_SCHEMA.getField("nullableUnion").schema());
    assertTrue(nullableUnionValue instanceof Integer);
    assertEquals(((Integer) nullableUnionValue).intValue(), 10);

    // Test union with two branches: something else + null
    assertNull(RowToAvroConverter.convertToUnion(null, IntegerType, AVRO_SCHEMA.getField("nullableUnion2").schema()));
    Object nullableUnion2Value =
        RowToAvroConverter.convertToUnion(10, IntegerType, AVRO_SCHEMA.getField("nullableUnion2").schema());
    assertTrue(nullableUnion2Value instanceof Integer);
    assertEquals(((Integer) nullableUnion2Value).intValue(), 10);

    // Test union with two branches: int + long
    Object intLongUnion =
        RowToAvroConverter.convertToUnion(10L, LongType, AVRO_SCHEMA.getField("intLongUnion").schema());
    assertTrue(intLongUnion instanceof Long);
    assertEquals(((Long) intLongUnion).longValue(), 10L);

    // Test union with two branches: long + int
    Object longIntUnion =
        RowToAvroConverter.convertToUnion(10L, LongType, AVRO_SCHEMA.getField("intLongUnion").schema());
    assertTrue(longIntUnion instanceof Long);
    assertEquals(((Long) longIntUnion).longValue(), 10L);

    // Test union with two branches: float + double
    Object floatDoubleUnion =
        RowToAvroConverter.convertToUnion(0.5, DoubleType, AVRO_SCHEMA.getField("floatDoubleUnion").schema());
    assertTrue(floatDoubleUnion instanceof Double);
    assertEquals((Double) floatDoubleUnion, 0.5, 0.001);

    // Test union with two branches: double + float
    Object doubleFloatUnion =
        RowToAvroConverter.convertToUnion(0.5, DoubleType, AVRO_SCHEMA.getField("doubleFloatUnion").schema());
    assertTrue(doubleFloatUnion instanceof Double);
    assertEquals((Double) doubleFloatUnion, 0.5, 0.001);

    // Test complex union without null
    Object complexNonNullableUnion1 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { null, 0.5f, null }, UNION_STRUCT_DOUBLE_FLOAT_STRING),
        UNION_STRUCT_DOUBLE_FLOAT_STRING,
        AVRO_SCHEMA.getField("complexNonNullableUnion").schema());
    assertTrue(complexNonNullableUnion1 instanceof Float);
    assertEquals((Float) complexNonNullableUnion1, 0.5f, 0.001f);

    Object complexNonNullableUnion2 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { 0.5, null, null }, UNION_STRUCT_DOUBLE_FLOAT_STRING),
        UNION_STRUCT_DOUBLE_FLOAT_STRING,
        AVRO_SCHEMA.getField("complexNonNullableUnion").schema());
    assertTrue(complexNonNullableUnion2 instanceof Double);
    assertEquals((Double) complexNonNullableUnion2, 0.5, 0.001);

    Object complexNonNullableUnion3 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { null, null, STRING_VALUE }, UNION_STRUCT_DOUBLE_FLOAT_STRING),
        UNION_STRUCT_DOUBLE_FLOAT_STRING,
        AVRO_SCHEMA.getField("complexNonNullableUnion").schema());
    assertTrue(complexNonNullableUnion3 instanceof String);
    assertEquals(complexNonNullableUnion3, STRING_VALUE);

    // Test complex union with null in first branch
    Object complexNullableUnion1_1 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { null, 10 }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion1").schema());
    assertTrue(complexNullableUnion1_1 instanceof Integer);
    assertEquals(((Integer) complexNullableUnion1_1).intValue(), 10);

    Object complexNullableUnion1_2 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { STRING_VALUE, null }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion1").schema());
    assertTrue(complexNullableUnion1_2 instanceof String);
    assertEquals(complexNullableUnion1_2, STRING_VALUE);

    Object complexNullableUnion1_3 = RowToAvroConverter
        .convertToUnion(null, UNION_STRUCT_STRING_INT, AVRO_SCHEMA.getField("complexNullableUnion1").schema());
    assertNull(complexNullableUnion1_3);

    // Test complex union with null in second branch
    Object complexNullableUnion2_1 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { null, 10 }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion2").schema());
    assertTrue(complexNullableUnion2_1 instanceof Integer);
    assertEquals(((Integer) complexNullableUnion2_1).intValue(), 10);

    Object complexNullableUnion2_2 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { STRING_VALUE, null }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion2").schema());
    assertTrue(complexNullableUnion2_2 instanceof String);
    assertEquals(complexNullableUnion2_2, STRING_VALUE);

    Object complexNullableUnion2_3 = RowToAvroConverter
        .convertToUnion(null, UNION_STRUCT_STRING_INT, AVRO_SCHEMA.getField("complexNullableUnion2").schema());
    assertNull(complexNullableUnion2_3);

    // Test complex union with null in third branch
    Object complexNullableUnion3_1 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { null, 10 }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion3").schema());
    assertTrue(complexNullableUnion3_1 instanceof Integer);
    assertEquals(((Integer) complexNullableUnion3_1).intValue(), 10);

    Object complexNullableUnion3_2 = RowToAvroConverter.convertToUnion(
        new GenericRowWithSchema(new Object[] { STRING_VALUE, null }, UNION_STRUCT_STRING_INT),
        UNION_STRUCT_STRING_INT,
        AVRO_SCHEMA.getField("complexNullableUnion3").schema());
    assertTrue(complexNullableUnion3_2 instanceof String);
    assertEquals(complexNullableUnion3_2, STRING_VALUE);

    Object complexNullableUnion3_3 = RowToAvroConverter
        .convertToUnion(null, UNION_STRUCT_STRING_INT, AVRO_SCHEMA.getField("complexNullableUnion3").schema());
    assertNull(complexNullableUnion3_3);

    // At least one branch must be non-null
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.convertToUnion(
            new GenericRowWithSchema(new Object[] { null, null }, UNION_STRUCT_STRING_INT),
            UNION_STRUCT_STRING_INT,
            AVRO_SCHEMA.getField("complexNullableUnion3").schema()));
  }

  @Test
  public void testValidateLogicalType() {
    assertEquals(
        RowToAvroConverter.validateLogicalType(DATE_TYPE, LogicalTypes.date(), LogicalTypes.timeMillis()),
        LogicalTypes.date());

    // Logical type must match the Spark type if it is mandatory
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.validateLogicalType(DATE_TYPE, LogicalTypes.timeMillis()));

    // Logical type must be present in the Avro schema if it is mandatory
    assertThrows(
        IllegalArgumentException.class,
        () -> RowToAvroConverter.validateLogicalType(Schema.create(Schema.Type.LONG), LogicalTypes.timeMillis()));

    // Logical type might not be present in the Avro schema if it is optional
    assertNull(
        RowToAvroConverter.validateLogicalType(Schema.create(Schema.Type.LONG), false, LogicalTypes.timeMillis()));
  }
}
