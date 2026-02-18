package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_INPUT_FILE_DATA_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_ZSTD_COMPRESSION_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.POLL_JOB_STATUS_INTERVAL_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;

import com.google.common.base.CaseFormat;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.etl.ETLUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


public class TestWriteUtils {
  private static final Logger LOGGER = LogManager.getLogger(TestWriteUtils.class);
  public static final int DEFAULT_USER_DATA_RECORD_COUNT = 100;
  public static final String DEFAULT_USER_DATA_VALUE_PREFIX = "test_name_";

  // Key / Value Schema
  public static final Schema STRING_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/primitive/String.avsc"));
  public static final Schema INT_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/primitive/Int.avsc"));
  public static final Schema USER_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/User.avsc"));
  public static final Schema USER_WITH_DEFAULT_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UserWithDefault.avsc"));

  public static final Schema SINGLE_FIELD_RECORD_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/SingleFieldRecord.avsc"));

  public static final Schema TWO_FIELDS_RECORD_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/TwoFieldsRecord.avsc"));

  public static final Schema SIMPLE_USER_WITH_DEFAULT_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/SimpleUserWithDefault.avsc"));
  public static final Schema USER_WITH_FLOAT_ARRAY_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UserWithFloatArray.avsc"));
  public static final Schema USER_WITH_NESTED_RECORD_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UserWithNestedRecord.avsc"));

  public static final Schema USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UserWithNestedRecordAndDefault.avsc"));

  public static final Schema USER_WITH_STRING_MAP_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UserWithStringMap.avsc"));

  public static final Schema NAME_RECORD_V1_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV1.avsc"));
  public static final Schema NAME_RECORD_V2_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV2.avsc"));
  public static final Schema NAME_RECORD_V3_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV3.avsc"));
  public static final Schema NAME_RECORD_V4_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV4.avsc"));
  public static final Schema NAME_RECORD_V5_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV5.avsc"));
  public static final Schema NAME_RECORD_V6_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV6.avsc"));
  public static final Schema NAME_RECORD_V7_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV7.avsc"));
  public static final Schema NAME_RECORD_V8_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV8.avsc"));
  public static final Schema NAME_RECORD_V9_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV9.avsc"));
  public static final Schema NAME_RECORD_V10_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV10.avsc"));
  public static final Schema NAME_RECORD_V11_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/NameV11.avsc"));

  public static final Schema UNION_RECORD_V1_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UnionV1.avsc"));
  public static final Schema UNION_RECORD_V2_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UnionV2.avsc"));
  public static final Schema UNION_RECORD_V3_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("valueSchema/UnionV3.avsc"));

  // ETL Schema
  public static final Schema ETL_KEY_SCHEMA = AvroCompatibilityHelper.parse(loadSchemaFileFromResource("etl/Key.avsc"));
  public static final Schema ETL_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("etl/Value.avsc"));
  public static final Schema ETL_UNION_VALUE_WITH_NULL_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("etl/UnionValueWithNull.avsc"));
  public static final Schema ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileFromResource("etl/UnionValueWithoutNull.avsc"));

  // Partial Update Schema
  public static final Schema NAME_RECORD_V1_UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V1_SCHEMA);

  // Push Input Folder Schema
  public static final Schema INT_TO_STRING_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(INT_SCHEMA).setValueSchema(STRING_SCHEMA).build();
  public static final Schema INT_TO_INT_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(INT_SCHEMA).setValueSchema(INT_SCHEMA).build();
  public static final Schema STRING_TO_STRING_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(STRING_SCHEMA).build();
  public static final Schema STRING_TO_STRING_WITH_TIMESTAMP = new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
      .setValueSchema(STRING_SCHEMA)
      .setFieldSchema(DEFAULT_RMD_FIELD_PROP, Schema.create(Schema.Type.LONG))
      .build();

  public static final Schema STRING_TO_STRING_WITH_TIMESTAMP_BYTES =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
          .setValueSchema(STRING_SCHEMA)
          .setFieldSchema(DEFAULT_RMD_FIELD_PROP, Schema.create(Schema.Type.BYTES))
          .build();

  public static final Schema STRING_TO_NAME_WITH_TIMESTAMP_RECORD_V1_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
          .setValueSchema(NAME_RECORD_V1_SCHEMA)
          .setFieldSchema(DEFAULT_RMD_FIELD_PROP, Schema.create(Schema.Type.BYTES))
          .build();

  public static final Schema STRING_TO_NAME_RECORD_V1_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V1_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V2_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V2_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V3_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V3_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V5_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V5_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V6_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V6_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V7_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V7_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V8_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V8_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V9_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V9_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V10_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V10_SCHEMA).build();
  public static final Schema STRING_TO_NAME_RECORD_V11_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V11_SCHEMA).build();
  private static final Schema[] STRING_TO_NAME_RECORD_SCHEMAS = new Schema[] { STRING_TO_NAME_RECORD_V1_SCHEMA,
      STRING_TO_NAME_RECORD_V2_SCHEMA, STRING_TO_NAME_RECORD_V3_SCHEMA, STRING_TO_NAME_RECORD_V5_SCHEMA,
      STRING_TO_NAME_RECORD_V6_SCHEMA, STRING_TO_NAME_RECORD_V7_SCHEMA, STRING_TO_NAME_RECORD_V8_SCHEMA,
      STRING_TO_NAME_RECORD_V9_SCHEMA, STRING_TO_NAME_RECORD_V10_SCHEMA, STRING_TO_NAME_RECORD_V11_SCHEMA };

  public static final Schema STRING_TO_NAME_RECORD_V1_UPDATE_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(NAME_RECORD_V1_UPDATE_SCHEMA).build();
  public static final Schema STRING_TO_STRING_WITH_EXTRA_FIELD_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA)
          .setValueSchema(STRING_SCHEMA)
          .setFieldSchema("age", INT_SCHEMA)
          .build();
  public static final Schema STRING_TO_USER_WITH_STRING_MAP_SCHEMA =
      new PushInputSchemaBuilder().setKeySchema(STRING_SCHEMA).setValueSchema(USER_WITH_STRING_MAP_SCHEMA).build();

  public static File getTempDataDirectory() {
    return Utils.getTempDataDirectory();
  }

  public static GenericRecord renderNameRecord(Schema schema, int i) {

    // Key
    GenericRecord keyValueRecord = new GenericData.Record(schema);
    keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, String.valueOf(i));

    // Value
    Schema valueSchema = schema.getField(DEFAULT_VALUE_FIELD_PROP).schema();
    valueSchema.getFields().get(0).name();
    GenericRecord valueRecord = new GenericData.Record(schema.getField(DEFAULT_VALUE_FIELD_PROP).schema());
    for (Schema.Field field: valueSchema.getFields()) {
      Object value = null;
      switch (field.schema().getType()) {
        case STRING:
          // Camel case field name to snake case value
          value = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.name()) + "_" + i;
          break;
        case INT:
        case LONG:
          value = i;
          break;
        case FLOAT:
        case DOUBLE:
          value = (double) i;
          break;
        case BOOLEAN:
          value = true;
          break;
        default:
          break;
      }
      valueRecord.put(field.name(), value);
    }
    keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord);

    return keyValueRecord;
  }

  public static int countStringToNameRecordSchemas() {
    return STRING_TO_NAME_RECORD_SCHEMAS.length;
  }

  public static Schema getStringToNameRecordSchema(int version) {
    return STRING_TO_NAME_RECORD_SCHEMAS[version];
  }

  public static Schema writeSimpleAvroFileWithStringToStringSchema(File parentDir) throws IOException {
    return writeSimpleAvroFileWithStringToStringSchema(parentDir, DEFAULT_USER_DATA_RECORD_COUNT);
  }

  public static Schema writeSimpleAvroFileWithStringToStringAndTimestampSchema(File parentDir, long timestamp)
      throws IOException {
    return writeSimpleAvroFileWithStringToStringAndTimestampSchema(
        parentDir,
        DEFAULT_USER_DATA_RECORD_COUNT,
        "string2string_with_timestamp.avro",
        timestamp);
  }

  public static Schema writeSimpleAvroFileWithStringToStringAndTimestampSchema(
      File parentDir,
      int recordCount,
      String fileName,
      long timestamp) throws IOException {
    return writeAvroFile(parentDir, fileName, STRING_TO_STRING_WITH_TIMESTAMP, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
        user.put(DEFAULT_RMD_FIELD_PROP, timestamp);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToStringAndTimestampSchema(File parentDir, byte[] timestamp)
      throws IOException {
    return writeAvroFile(
        parentDir,
        "string2string_with_timestamp.avro",
        STRING_TO_STRING_WITH_TIMESTAMP_BYTES,
        (recordSchema, writer) -> {
          for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);
            user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
            user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
            user.put(DEFAULT_RMD_FIELD_PROP, ByteBuffer.wrap(timestamp));
            writer.append(user);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithStringToStringSchema(File parentDir, int recordCount) throws IOException {
    return writeSimpleAvroFileWithStringToStringSchema(parentDir, recordCount, "string2string.avro");
  }

  public static Schema writeSimpleAvroFileWithStringToStringSchema(File parentDir, int recordCount, String fileName)
      throws IOException {
    return writeAvroFile(parentDir, fileName, STRING_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToStringWithExtraSchema(File parentDir) throws IOException {
    return writeAvroFile(
        parentDir,
        "string2string_extra_field.avro",
        STRING_TO_STRING_WITH_EXTRA_FIELD_SCHEMA,
        (recordSchema, writer) -> {
          for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);
            user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
            user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
            user.put("age", i);
            writer.append(user);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithStringToStringSchema(File parentDir, int recordCount, int recordSizeMin)
      throws IOException {
    char[] chars = new char[recordSizeMin];
    return writeAvroFile(parentDir, "string2string.avro", STRING_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        Arrays.fill(chars, String.valueOf(i).charAt(0));
        user.put(DEFAULT_VALUE_FIELD_PROP, String.copyValueOf(chars));
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToV3Schema(File parentDir, int recordCount, int recordSizeMin)
      throws IOException {
    char[] chars = new char[recordSizeMin];
    RandomRecordGenerator recordGenerator = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    return writeAvroFile(parentDir, "string2v4schema.avro", STRING_TO_NAME_RECORD_V3_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        Arrays.fill(chars, String.valueOf(i).charAt(0));
        GenericRecord record = (GenericRecord) recordGenerator.randomGeneric(NAME_RECORD_V3_SCHEMA, genConfig);
        user.put(DEFAULT_VALUE_FIELD_PROP, record);
        writer.append(user);
      }
    });
  }

  public static Schema writeAlternateSimpleAvroFileWithStringToStringSchema(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "string2string.avro", STRING_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, "alternate_test_name_" + i);
        writer.append(user);
      }
    });
  }

  /**
   * This file overrides half of the value in {@link #writeSimpleAvroFileWithStringToStringSchema(File)}
   * and add some new values.
   * It's designed to test incremental push
   */
  public static Schema writeSimpleAvroFileWithStringToStringSchema2(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "string2string.avro", STRING_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 51; i <= 150; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + (i * 2));
        writer.append(user);
      }
    });
  }

  /**
   * This file add some new value in {@link #writeSimpleAvroFileWithStringToStringSchema(File)}
   * It's designed to test incremental push
   */
  public static Schema writeSimpleAvroFileWithString2StringSchema3(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "string2string.avro", STRING_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 51; i <= 200; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + (i * 3));
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithDuplicateKey(File parentDir) throws IOException {
    return writeAvroFile(
        parentDir,
        "duplicate_key_user.avro",
        STRING_TO_STRING_SCHEMA,
        (recordSchema, avroFileWriter) -> {
          for (int i = 0; i < 100; i++) {
            GenericRecord user = new GenericData.Record(recordSchema);
            user.put(DEFAULT_KEY_FIELD_PROP, i % 10 == 0 ? "0" : Integer.toString(i));
            user.put(DEFAULT_VALUE_FIELD_PROP, "test_name" + i);
            avroFileWriter.append(user);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithCustomSize(
      File parentDir,
      int numberOfRecords,
      int minValueSize,
      int maxValueSize) throws IOException {
    return writeAvroFile(
        parentDir,
        "string2string_large.avro",
        STRING_TO_STRING_SCHEMA,
        (recordSchema, avroFileWriter) -> {
          int sizeRange = maxValueSize - minValueSize;
          for (int i = 0; i < numberOfRecords; i++) {
            int sizeForThisRecord = minValueSize + sizeRange / numberOfRecords * (i + 1);
            GenericRecord user = new GenericData.Record(recordSchema);
            user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
            char[] chars = new char[sizeForThisRecord];
            Arrays.fill(chars, Integer.toString(i).charAt(0));
            Utf8 utf8Value = new Utf8(new String(chars));
            user.put(DEFAULT_VALUE_FIELD_PROP, utf8Value);
            avroFileWriter.append(user);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithIntToStringSchema(File parentDir) throws IOException {
    return writeSimpleAvroFileWithIntToStringSchema(parentDir, "name ", DEFAULT_USER_DATA_RECORD_COUNT);
  }

  public static Schema writeSimpleAvroFileWithIntToStringSchema(File parentDir, String customValue, int numKeys)
      throws IOException {
    return writeAvroFile(parentDir, "int2string.avro", INT_TO_STRING_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= numKeys; ++i) {
        GenericRecord i2s = new GenericData.Record(recordSchema);
        i2s.put(DEFAULT_KEY_FIELD_PROP, i);
        i2s.put(DEFAULT_VALUE_FIELD_PROP, customValue + i);
        writer.append(i2s);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithIntToIntSchema(File parentDir, int numKeys) throws IOException {
    return writeAvroFile(parentDir, "int2int.avro", INT_TO_INT_SCHEMA, (recordSchema, writer) -> {
      for (int i = 1; i <= numKeys; ++i) {
        GenericRecord i2s = new GenericData.Record(recordSchema);
        i2s.put(DEFAULT_KEY_FIELD_PROP, i);
        i2s.put(DEFAULT_VALUE_FIELD_PROP, i);
        writer.append(i2s);
      }
    });
  }

  public static void writeInvalidAvroFile(File parentDir, String fileName) throws IOException {
    PrintWriter writer = new PrintWriter(parentDir.getAbsolutePath() + "/" + fileName, "UTF-8");
    writer.println("Invalid file");
    writer.close();
  }

  public static Schema writeEmptyAvroFile(File parentDir, Schema schema) throws IOException {
    return writeEmptyAvroFile(parentDir, "empty_file.avro", schema);
  }

  public static Schema writeEmptyAvroFile(File parentDir, String fileName, Schema schema) throws IOException {
    return writeAvroFile(parentDir, fileName, schema, (recordSchema, avroFileWriter) -> {
      // No-op so that the file is empty
    });
  }

  public static Schema writeSimpleAvroFileWithStringToNameRecordV1Schema(File parentDir) throws IOException {
    return writeSimpleAvroFileWithStringToNameRecordV1Schema(parentDir, DEFAULT_USER_DATA_RECORD_COUNT);
  }

  public static Schema writeSimpleAvroFileWithStringToNameRecordV1Schema(File parentDir, int recordCount)
      throws IOException {
    return writeSimpleAvroFileWithStringToNameRecordSchema(parentDir, STRING_TO_NAME_RECORD_V1_SCHEMA, recordCount);
  }

  public static Schema writeSimpleAvroFileWithStringToNameRecordSchema(File parentDir, Schema schema, int recordCount)
      throws IOException {
    return writeSimpleAvroFile(parentDir, schema, i -> renderNameRecord(schema, i), recordCount);
  }

  public static Schema writeSimpleAvroFile(
      File parentDir,
      Schema schema,
      Function<Integer, GenericRecord> recordProvider,
      int recordCount) throws IOException {
    return writeAvroFile(parentDir, "string2record.avro", schema, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        writer.append(recordProvider.apply(i));
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToNameRecordV2Schema(File parentDir) throws IOException {
    String firstName = "first_name_";
    String lastName = "last_name_";

    return writeSimpleAvroFile(parentDir, STRING_TO_NAME_RECORD_V2_SCHEMA, i -> {
      GenericRecord keyValueRecord = new GenericData.Record(STRING_TO_NAME_RECORD_V2_SCHEMA);
      keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, String.valueOf(i)); // Key
      GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V2_SCHEMA);
      valueRecord.put("firstName", firstName + i);
      valueRecord.put("lastName", lastName + i);
      valueRecord.put("age", -1);
      keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord); // Value
      return keyValueRecord;
    });
  }

  public static Schema writeSimpleAvroFile(
      File parentDir,
      Schema schema,
      Function<Integer, GenericRecord> recordProvider) throws IOException {
    return writeSimpleAvroFile(parentDir, schema, recordProvider, DEFAULT_USER_DATA_RECORD_COUNT);
  }

  public static Schema writeSimpleAvroFileWithStringToUserWithStringMapSchema(File parentDir, int itemsPerRecord)
      throws IOException {
    String valuePayloadBase = "1234567890";
    StringBuilder valuePayloadBuilder = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      valuePayloadBuilder.append(valuePayloadBase);
    }
    return writeAvroFile(
        parentDir,
        "many_strings.avro",
        STRING_TO_USER_WITH_STRING_MAP_SCHEMA,
        (recordSchema, writer) -> {
          for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; ++i) {
            GenericRecord keyValueRecord = new GenericData.Record(recordSchema);
            keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, String.valueOf(i)); // Key
            GenericRecord valueRecord = new GenericData.Record(USER_WITH_STRING_MAP_SCHEMA);
            valueRecord.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i)); // DEFAULT_KEY_FIELD_PROP is the key
            Map<String, String> stringMap = new HashMap<>();
            for (int j = 0; j < itemsPerRecord; j++) {
              stringMap.put("item_" + j, valuePayloadBuilder.toString());
            }
            valueRecord.put(DEFAULT_VALUE_FIELD_PROP, stringMap);
            valueRecord.put("age", i);
            keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord); // Value
            writer.append(keyValueRecord);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(File parentDir) throws IOException {
    return writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(parentDir, 1, 100);
  }

  public static Schema writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(
      File parentDir,
      int startIndex,
      int endIndex) throws IOException {
    return writeAvroFile(
        parentDir,
        "string2record.avro",
        STRING_TO_NAME_RECORD_V1_UPDATE_SCHEMA,
        (recordSchema, writer) -> {
          String firstName = "first_name_";
          String lastName = "last_name_";
          for (int i = startIndex; i <= endIndex; ++i) {
            GenericRecord keyValueRecord = new GenericData.Record(recordSchema);
            keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, String.valueOf(i)); // Key
            GenericRecord valueRecord =
                new UpdateBuilderImpl(NAME_RECORD_V1_UPDATE_SCHEMA).setNewFieldValue("firstName", firstName + i)
                    .setNewFieldValue("lastName", lastName + i)
                    .build();
            keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord); // Value
            writer.append(keyValueRecord);
          }
        });
  }

  public static Schema writeSimpleAvroFileWithASchemaWithAWrongDefaultValue(File parentDir, int numberOfRecords)
      throws IOException {
    final String schemaWithWrongDefaultValue = "{\n" + "  \"namespace\": \"example.avro\",\n"
        + "  \"type\": \"record\",\n" + "  \"name\": \"SimpleRecord\",\n" + "  \"fields\": [\n"
        + "     {\"name\": \"key\", \"type\": \"string\"},\n" + "     {\"name\": \"value\", \"type\": {\n"
        + "        \"name\": \"RecordWithWrongDefault\",\n" + "        \"type\": \"record\",\n"
        + "        \"fields\": [\n" + "         {\"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\"},\n"
        + "         {\"name\": \"score\", \"type\": \"float\", \"default\": 0}\n" + "       ]}\n" + "     }\n"
        + "   ]\n" + "}";
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSON(schemaWithWrongDefaultValue, false);
    return writeAvroFile(parentDir, "record_with_wrong_default.avro", schema, (recordSchema, avroFileWriter) -> {
      for (int i = 0; i < numberOfRecords; i++) {
        GenericRecord simpleRecord = new GenericData.Record(recordSchema);
        simpleRecord.put("key", Integer.toString(i));
        GenericRecord value = new GenericData.Record(recordSchema.getField("value").schema());
        value.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        value.put("score", 100.0f);
        simpleRecord.put("value", value);
        avroFileWriter.append(simpleRecord);
      }
    });
  }

  public static Schema writeAvroFileWithManyFloatsAndCustomTotalSize(
      File parentDir,
      int numberOfRecords,
      int minValueSize,
      int maxValueSize) throws IOException {
    return writeAvroFile(
        parentDir,
        "many_floats.avro",
        USER_WITH_FLOAT_ARRAY_SCHEMA,
        (recordSchema, avroFileWriter) -> {
          int sizeRange = maxValueSize - minValueSize;
          for (int i = 0; i < numberOfRecords; i++) {
            int sizeForThisRecord = minValueSize + sizeRange / numberOfRecords * (i + 1);
            avroFileWriter.append(getRecordWithFloatArray(recordSchema, i, sizeForThisRecord));
          }
        });
  }

  public static GenericRecord getRecordWithFloatArray(Schema recordSchema, int index, int size) {
    GenericRecord user = new GenericData.Record(recordSchema);
    user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(index)); // DEFAULT_KEY_FIELD_PROP is the key
    int numberOfFloats = size / 4;
    List<Float> floatsArray = new ArrayList<>();
    for (int j = 0; j < numberOfFloats; j++) {
      floatsArray.add(RandomGenUtils.getRandomFloat());
    }
    user.put(DEFAULT_VALUE_FIELD_PROP, floatsArray);
    user.put("age", index);
    return user;
  }

  private static Schema writeAvroFile(File parentDir, String fileName, Schema recordSchema, AvroFileWriter fileWriter)
      throws IOException {
    File file = new File(parentDir, fileName);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(recordSchema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(recordSchema, file);
      fileWriter.write(recordSchema, dataFileWriter);
    }
    return recordSchema;
  }

  public static KeyAndValueSchemas writeSimpleVsonFile(File parentDir) throws IOException {
    String vsonInteger = "\"int32\"";
    String vsonString = "\"string\"";

    writeVsonFile(vsonInteger, vsonString, parentDir, "simple_vson_file", (keySerializer, valueSerializer, writer) -> {
      for (int i = 0; i < 100; i++) {
        writer.append(
            new BytesWritable(keySerializer.toBytes(i)),
            new BytesWritable(valueSerializer.toBytes(String.valueOf(i + 100))));
      }
    });
    return new KeyAndValueSchemas(VsonAvroSchemaAdapter.parse(vsonInteger), VsonAvroSchemaAdapter.parse(vsonString));
  }

  public enum TestRecordType {
    NEARLINE, OFFLINE
  }

  public enum TestTargetedField {
    WEBSITE_URL, LOGO, INDUSTRY
  }

  public static Schema writeSchemaWithUnknownFieldIntoAvroFile(File parentDir) throws IOException {
    String schemaWithSymbolDocStr = loadFileAsString("SchemaWithSymbolDoc.avsc");
    Schema schemaWithSymbolDoc = Schema.parse(schemaWithSymbolDocStr);
    File file = new File(parentDir, "schema_with_unknown_field.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schemaWithSymbolDoc);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schemaWithSymbolDoc, file);

      for (int i = 1; i <= 10; ++i) {
        GenericRecord newRecord = new GenericData.Record(schemaWithSymbolDoc);
        GenericRecord keyRecord = new GenericData.Record(schemaWithSymbolDoc.getField("key").schema());
        keyRecord.put("memberId", (long) i);
        Schema sourceSchema = keyRecord.getSchema().getField("source").schema();
        if (i % 2 == 0) {
          keyRecord
              .put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, TestRecordType.NEARLINE.toString()));
        } else {
          keyRecord
              .put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, TestRecordType.OFFLINE.toString()));
        }

        GenericRecord valueRecord = new GenericData.Record(schemaWithSymbolDoc.getField("value").schema());
        Schema targetSchema = valueRecord.getSchema().getField("targetedField").schema();
        valueRecord.put("priority", i);
        if (i % 3 == 0) {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, TestTargetedField.WEBSITE_URL.toString()));
        } else if (i % 3 == 1) {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, TestTargetedField.LOGO.toString()));
        } else {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, TestTargetedField.INDUSTRY.toString()));
        }

        newRecord.put("key", keyRecord);
        newRecord.put("value", valueRecord);
        dataFileWriter.append(newRecord);
      }
    }

    /**
     * return a schema without symbolDoc field so that the venice store is created with schema
     * that doesn't contain symbolDoc but the files in HDFS has symbolDoc.
     */
    String schemaWithoutSymbolDocStr = loadFileAsString("SchemaWithoutSymbolDoc.avsc");
    return AvroCompatibilityHelper.parse(schemaWithoutSymbolDocStr);
  }

  // write vson byte (int 8) and short (int16) to a file
  public static KeyAndValueSchemas writeVsonByteAndShort(File parentDir) throws IOException {
    String vsonByte = "\"int8\"";
    String vsonShort = "\"int16\"";

    writeVsonFile(
        vsonByte,
        vsonShort,
        parentDir,
        "vson_byteAndShort_file",
        (keySerializer, valueSerializer, writer) -> {
          for (int i = 0; i < 100; i++) {
            writer.append(
                new BytesWritable(keySerializer.toBytes((byte) i)),
                new BytesWritable(valueSerializer.toBytes((short) (i - 50))));
          }
        });
    return new KeyAndValueSchemas(VsonAvroSchemaAdapter.parse(vsonByte), VsonAvroSchemaAdapter.parse(vsonShort));
  }

  public static KeyAndValueSchemas writeComplexVsonFile(File parentDir) throws IOException {
    String vsonInteger = "\"int32\"";
    String vsonString = "{\"member_id\":\"int32\", \"score\":\"float32\"}";

    Map<String, Object> record = new HashMap<>();
    writeVsonFile(vsonInteger, vsonString, parentDir, "complex_vson-file", (keySerializer, valueSerializer, writer) -> {
      for (int i = 0; i < 100; i++) {
        record.put("member_id", i + 100);
        record.put("score", i % 10 != 0 ? (float) i : null); // allow to have optional field
        writer.append(new BytesWritable(keySerializer.toBytes(i)), new BytesWritable(valueSerializer.toBytes(record)));
      }
    });
    return new KeyAndValueSchemas(VsonAvroSchemaAdapter.parse(vsonInteger), VsonAvroSchemaAdapter.parse(vsonString));
  }

  public static Pair<Schema, Schema> writeSimpleVsonFileWithUserSchema(File parentDir) throws IOException {
    String vsonKey = "\"string\"";
    String vsonValue = "{\"name\":\"string\", \"age\":\"int32\"}";

    writeVsonFile(vsonKey, vsonValue, parentDir, "complex_user_vson-file", (keySerializer, valueSerializer, writer) -> {
      for (int i = 1; i <= 100; i++) {
        Map<String, Object> valueRecord = new HashMap<>();
        valueRecord.put("name", DEFAULT_USER_DATA_VALUE_PREFIX + i);
        valueRecord.put("age", i);
        writer.append(
            new BytesWritable(keySerializer.toBytes(Integer.toString(i))),
            new BytesWritable(valueSerializer.toBytes(valueRecord)));
      }
    });
    return new Pair<>(VsonAvroSchemaAdapter.parse(vsonKey), VsonAvroSchemaAdapter.parse(vsonValue));
  }

  public static KeyAndValueSchemas writeMultiLevelVsonFile(File parentDir) throws IOException {
    String vsonKeyStr = "\"int32\"";
    String vsonValueStr = "{\"level1\":{\"level21\":{\"field1\":\"int32\"}, \"level22\":{\"field2\":\"int32\"}}}";
    Map<String, Object> record = new HashMap<>();
    writeVsonFile(
        vsonKeyStr,
        vsonValueStr,
        parentDir,
        "multilevel_vson_file",
        (keySerializer, valueSerializer, writer) -> {
          for (int i = 0; i < 100; i++) {
            Map<String, Object> record21 = new HashMap<>();
            record21.put("field1", i + 100);
            Map<String, Object> record22 = new HashMap<>();
            record22.put("field2", i + 100);
            Map<String, Object> record1 = new HashMap<>();
            record1.put("level21", record21);
            record1.put("level22", record22);
            record.put("level1", record1);

            writer.append(
                new BytesWritable(keySerializer.toBytes(i)),
                new BytesWritable(valueSerializer.toBytes(record)));
          }
        });
    return new KeyAndValueSchemas(VsonAvroSchemaAdapter.parse(vsonKeyStr), VsonAvroSchemaAdapter.parse(vsonValueStr));
  }

  public static Pair<VsonSchema, VsonSchema> writeMultiLevelVsonFile2(File parentDir) throws IOException {
    String vsonKeyStr = "\"int32\"";
    String vsonValueStr =
        "{\"keys\":[{\"type\":\"string\", \"value\":\"string\"}], \"recs\":[{\"member_id\":\"int32\", \"score\":\"float32\"}]}";
    writeVsonFile(
        vsonKeyStr,
        vsonValueStr,
        parentDir,
        "multilevel_vson_file2",
        (keySerializer, valueSerializer, writer) -> {
          for (int i = 0; i < 100; i++) {
            // construct value
            Map<String, Object> valueRecord = new HashMap<>();
            List<Map<String, Object>> innerList1 = new ArrayList<>();
            List<Map<String, Object>> innerList2 = new ArrayList<>();
            Map<String, Object> innerMap1 = new HashMap<>();
            Map<String, Object> innerMap2 = new HashMap<>();

            innerMap1.put("type", String.valueOf(i));
            innerMap1.put("value", String.valueOf(i + 100));
            innerList1.add(innerMap1);
            innerMap2.put("member_id", i);
            innerMap2.put("score", (float) i);
            innerList2.add(innerMap2);
            valueRecord.put("keys", innerList1);
            valueRecord.put("recs", innerList2);

            writer.append(
                new BytesWritable(keySerializer.toBytes(i)),
                new BytesWritable(valueSerializer.toBytes(valueRecord)));
          }
        });
    return new Pair<>(VsonSchema.parse(vsonKeyStr), VsonSchema.parse(vsonValueStr));
  }

  private static void writeVsonFile(
      String keySchemaStr,
      String valueSchemaStr,
      File parentDir,
      String fileName,
      VsonFileWriter fileWriter) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(new Text("key.schema"), new Text(keySchemaStr));
    metadata.set(new Text("value.schema"), new Text(valueSchemaStr));

    VsonAvroSerializer keySerializer = VsonAvroSerializer.fromSchemaStr(keySchemaStr);
    VsonAvroSerializer valueSerializer = VsonAvroSerializer.fromSchemaStr(valueSchemaStr);

    try (SequenceFile.Writer writer = SequenceFile.createWriter(
        new Configuration(),
        SequenceFile.Writer.file(new Path(parentDir.toString(), fileName)),
        SequenceFile.Writer.keyClass(BytesWritable.class),
        SequenceFile.Writer.valueClass(BytesWritable.class),
        SequenceFile.Writer.metadata(metadata))) {
      fileWriter.write(keySerializer, valueSerializer, writer);
    }
  }

  private interface VsonFileWriter {
    void write(VsonAvroSerializer keySerializer, VsonAvroSerializer valueSerializer, SequenceFile.Writer writer)
        throws IOException;
  }

  private interface AvroFileWriter {
    void write(Schema recordSchema, DataFileWriter writer) throws IOException;
  }

  public static Properties defaultVPJPropsWithD2Routing(
      String parentRegionName,
      String parentRegionD2ZkAddress,
      Map<String, String> childRegionNamesToZkAddress,
      String parentControllerD2ServiceName,
      String childControllerD2ServiceName,
      String inputDirPath,
      String storeName,
      Map<String, String> pubSubClientConfigs) {
    final String controllerServiceName;
    parentRegionName = parentRegionName == null ? "parentRegion" : parentRegionName;
    Properties props = new Properties();
    props.putAll(pubSubClientConfigs);
    if (parentRegionD2ZkAddress != null) {
      controllerServiceName = parentControllerD2ServiceName;
      props.put(PARENT_CONTROLLER_REGION_NAME, parentRegionName);
      props.put(D2_ZK_HOSTS_PREFIX + parentRegionName, parentRegionD2ZkAddress);
      props.put(MULTI_REGION, true);
    } else {
      controllerServiceName = childControllerD2ServiceName;
      props.put(MULTI_REGION, false);
    }

    props.put(VENICE_DISCOVER_URL_PROP, String.format("d2://%s", controllerServiceName));
    props.put(SOURCE_GRID_FABRIC, childRegionNamesToZkAddress.entrySet().iterator().next().getKey());

    childRegionNamesToZkAddress.forEach(
        (childRegionIdentifier, childRegionD2ZkAddress) -> props
            .put(D2_ZK_HOSTS_PREFIX + childRegionIdentifier, childRegionD2ZkAddress));

    return defaultVPJPropsInternal(props, inputDirPath, storeName);
  }

  public static Properties defaultVPJProps(
      String veniceUrl,
      String inputDirPath,
      String storeName,
      Map<String, String> pubSubClientConfigs) {
    Properties props = new Properties();
    props.put(VENICE_DISCOVER_URL_PROP, veniceUrl);
    props.putAll(pubSubClientConfigs);
    return defaultVPJPropsInternal(props, inputDirPath, storeName);
  }

  private static Properties defaultVPJPropsInternal(Properties props, String inputDirPath, String storeName) {
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(INPUT_PATH_PROP, inputDirPath);
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(POLL_JOB_STATUS_INTERVAL_MS, 10);
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(CONTROLLER_REQUEST_RETRY_ATTEMPTS, "5");
    return props;
  }

  public static String loadFileAsString(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  public static String loadSchemaFileFromResource(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(TestWriteUtils.class.getClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  public static String loadFileAsStringQuietlyWithErrorLogged(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOGGER.error(e);
      return null;
    }
  }

  public static void updateStore(String storeName, ControllerClient controllerClient, UpdateStoreQueryParams params) {
    ControllerResponse controllerResponse = controllerClient.retryableRequest(5, c -> c.updateStore(storeName, params));
    Assert.assertFalse(
        controllerResponse.isError(),
        "The UpdateStore response returned an error: " + controllerResponse.getError());
  }

  public static Schema writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
      File parentDir,
      String file,
      long inputFileDataSize,
      ByteBuffer zstdDictionary,
      Schema avroSchema) throws IOException {
    return writeAvroFile(parentDir, file, avroSchema, (recordSchema, writer) -> {
      GenericRecord user = new GenericData.Record(recordSchema);
      user.put(KEY_INPUT_FILE_DATA_SIZE, inputFileDataSize);
      if (zstdDictionary != null) {
        user.put(KEY_ZSTD_COMPRESSION_DICTIONARY, zstdDictionary);
      }
      writer.append(user);
    });
  }

  public static Schema writeETLFileWithUserSchema(File parentDir) throws IOException {
    return writeETLFileWithUserSchema(parentDir, false);
  }

  public static Schema writeETLFileWithUserSchema(File parentDir, boolean includeRmd) throws IOException {
    String fileName = "simple_etl_user.avro";
    Schema schema = includeRmd
        ? getETLFileSchemaWithRmd(ETL_KEY_SCHEMA, ETL_VALUE_SCHEMA)
        : getETLFileSchema(ETL_KEY_SCHEMA, ETL_VALUE_SCHEMA);
    return writeAvroFile(parentDir, fileName, schema, (recordSchema, writer) -> {
      for (int i = 1; i <= 50; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);

        GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);
        GenericRecord value = new GenericData.Record(ETL_VALUE_SCHEMA);

        key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        value.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);

        user.put("metadata", new HashMap<>());

        user.put("key", key);
        user.put("value", value);
        user.put("offset", (long) i);
        if (includeRmd) {
          user.put("rmd", 123456789L);
        }
        user.put("DELETED_TS", null);

        writer.append(user);
      }

      for (int i = 51; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);

        GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

        key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

        user.put("metadata", new HashMap<>());

        user.put("key", key);
        user.put("value", null);
        user.put("offset", (long) i);
        if (includeRmd) {
          user.put("rmd", 123456789L);
        }
        user.put("DELETED_TS", (long) i);

        writer.append(user);
      }
    });
  }

  public static Schema writeETLFileWithUserSchemaAndNullDefaultValue(File parentDir) throws IOException {
    String fileName = "simple_etl_user_with_default.avro";
    Schema schema = getETLFileSchemaWithNullDefaultValue(ETL_KEY_SCHEMA, ETL_VALUE_SCHEMA);
    AvroCompatibilityHelper.parse(schema.toString());
    return writeAvroFile(parentDir, fileName, schema, (recordSchema, writer) -> {
      for (int i = 1; i <= 50; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);

        GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);
        GenericRecord value = new GenericData.Record(ETL_VALUE_SCHEMA);

        key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        value.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);

        user.put("metadata", new HashMap<>());

        user.put("key", key);
        user.put("value", value);
        user.put("offset", (long) i);
        user.put("DELETED_TS", null);

        writer.append(user);
      }

      for (int i = 51; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);

        GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

        key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

        user.put("metadata", new HashMap<>());

        user.put("key", key);
        user.put("value", null);
        user.put("offset", (long) i);
        user.put("DELETED_TS", (long) i);

        writer.append(user);
      }
    });
  }

  public static Schema writeETLFileWithUnionWithNullSchema(File parentDir) throws IOException {
    return writeAvroFile(
        parentDir,
        "simple_etl_union_with_null.avro",
        getETLFileSchema(ETL_KEY_SCHEMA, ETL_UNION_VALUE_WITH_NULL_SCHEMA),
        (recordSchema, writer) -> {
          for (int i = 1; i <= 25; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", "string_" + i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 26; i <= 50; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 51; i <= 100; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", null);
            user.put("offset", (long) i);
            user.put("DELETED_TS", (long) i);

            writer.append(user);
          }
        });
  }

  public static Schema writeETLFileWithUnionWithoutNullSchema(File parentDir) throws IOException {

    return writeAvroFile(
        parentDir,
        "simple_etl_union_without_null.avro",
        getETLFileSchema(ETL_KEY_SCHEMA, ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA),
        (recordSchema, writer) -> {
          for (int i = 1; i <= 25; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", "string_" + i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 26; i <= 50; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 51; i <= 100; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(ETL_KEY_SCHEMA);

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", null);
            user.put("offset", (long) i);
            user.put("DELETED_TS", (long) i);

            writer.append(user);
          }
        });
  }

  public static Schema getETLFileSchema(Schema keySchema, Schema valueSchema) {
    Schema finalValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);
    return Schema.createRecord(
        "storeName_v1",
        "",
        "",
        false,
        Arrays.asList(
            AvroCompatibilityHelper.newField(null).setName(DEFAULT_KEY_FIELD_PROP).setSchema(keySchema).build(),
            AvroCompatibilityHelper.newField(null)
                .setName(DEFAULT_VALUE_FIELD_PROP)
                .setSchema(finalValueSchema)
                .build(),
            AvroCompatibilityHelper.newField(null).setName("offset").setSchema(Schema.create(Schema.Type.LONG)).build(),
            AvroCompatibilityHelper.newField(null)
                .setName("DELETED_TS")
                .setSchema(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))
                .build(),
            AvroCompatibilityHelper.newField(null)
                .setName("metadata")
                .setSchema(Schema.createMap(Schema.create(Schema.Type.STRING)))
                .build()));
  }

  public static Schema getETLFileSchemaWithRmd(Schema keySchema, Schema valueSchema) {
    Schema finalValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);
    return Schema.createRecord(
        "storeName_v1",
        "",
        "",
        false,
        Arrays.asList(
            AvroCompatibilityHelper.newField(null).setName(DEFAULT_KEY_FIELD_PROP).setSchema(keySchema).build(),
            AvroCompatibilityHelper.newField(null)
                .setName(DEFAULT_VALUE_FIELD_PROP)
                .setSchema(finalValueSchema)
                .build(),
            AvroCompatibilityHelper.newField(null).setName("offset").setSchema(Schema.create(Schema.Type.LONG)).build(),
            AvroCompatibilityHelper.newField(null).setName("rmd").setSchema(Schema.create(Schema.Type.LONG)).build(),
            AvroCompatibilityHelper.newField(null)
                .setName("DELETED_TS")
                .setSchema(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))
                .build(),
            AvroCompatibilityHelper.newField(null)
                .setName("metadata")
                .setSchema(Schema.createMap(Schema.create(Schema.Type.STRING)))
                .build()));
  }

  public static Schema getETLFileSchemaWithNullDefaultValue(Schema keySchema, Schema valueSchema) {
    Schema finalValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);
    return Schema.createRecord(
        "storeName_v1",
        "",
        "",
        false,
        Arrays.asList(
            AvroCompatibilityHelper.newField(null).setName(DEFAULT_KEY_FIELD_PROP).setSchema(keySchema).build(),
            AvroCompatibilityHelper.newField(null)
                .setName(DEFAULT_VALUE_FIELD_PROP)
                .setSchema(finalValueSchema)
                .setDefault(null)
                .build(),
            AvroCompatibilityHelper.newField(null).setName("offset").setSchema(Schema.create(Schema.Type.LONG)).build(),
            AvroCompatibilityHelper.newField(null)
                .setName("DELETED_TS")
                .setSchema(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)))
                .build(),
            AvroCompatibilityHelper.newField(null)
                .setName("metadata")
                .setSchema(Schema.createMap(Schema.create(Schema.Type.STRING)))
                .build()));
  }
}
