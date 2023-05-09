package com.linkedin.venice.utils;

import static com.linkedin.venice.hadoop.VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.hadoop.VenicePushJob.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.INPUT_PATH_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_INPUT_FILE_DATA_SIZE;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_ZSTD_COMPRESSION_DICTIONARY;
import static com.linkedin.venice.hadoop.VenicePushJob.MULTI_REGION;
import static com.linkedin.venice.hadoop.VenicePushJob.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.POLL_JOB_STATUS_INTERVAL_MS;
import static com.linkedin.venice.hadoop.VenicePushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.etl.ETLUtils;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.writer.VeniceWriter;
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
import java.util.function.Consumer;
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
  public static final Logger LOGGER = LogManager.getLogger(TestWriteUtils.class);
  public static final String USER_SCHEMA_STRING =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP
          + "\", \"type\": \"string\"},  " + "       { \"name\": \"" + DEFAULT_VALUE_FIELD_PROP
          + "\", \"type\": \"string\"},  " + "       { \"name\": \"age\", \"type\": \"int\" }" + "  ] " + " } ";

  public static final String ETL_KEY_SCHEMA_STRING = "{\n" + "    \"type\":\"record\",\n" + "    \"name\":\"key\",\n"
      + "    \"namespace\":\"com.linkedin.venice.testkey\",\n" + "    \"fields\":[\n" + "        {\n"
      + "            \"name\":\"" + DEFAULT_KEY_FIELD_PROP + "\",\n" + "            \"type\":\"string\"\n"
      + "        }\n" + "    ]\n" + "}";

  public static final String ETL_VALUE_SCHEMA_STRING = "{\n" + "    \"type\":\"record\",\n"
      + "    \"name\":\"value\",\n" + "    \"namespace\":\"com.linkedin.venice.testvalue\",\n" + "    \"fields\":[\n"
      + "        {\n" + "            \"name\":\"" + DEFAULT_VALUE_FIELD_PROP + "\",\n"
      + "            \"type\":\"string\"\n" + "        }\n" + "    ],\n" + "    \"version\":10\n" + "}";

  public static final String ETL_UNION_VALUE_SCHEMA_STRING_WITHOUT_NULL = "[\"int\", \"string\"]";

  public static final String ETL_UNION_VALUE_SCHEMA_STRING_WITH_NULL = "[\"int\", \"string\", \"null\"]";

  public static final String USER_SCHEMA_STRING_SIMPLE_WITH_DEFAULT =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP
          + "\", \"type\": \"string\", \"default\": \"\"},  " + "       { \"name\": \"" + DEFAULT_VALUE_FIELD_PROP
          + "\", \"type\": \"string\", \"default\": \"\"}" + "  ] " + " } ";

  public static final String USER_SCHEMA_STRING_WITH_DEFAULT = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\", \"default\": \"\"},  "
      + "       { \"name\": \"" + DEFAULT_VALUE_FIELD_PROP + "\", \"type\": \"string\", \"default\": \"\"},  "
      + "       { \"name\": \"age\", \"type\": \"int\", \"default\": 1 }" + "  ] " + " } ";

  public static final String USER_SCHEMA_WITH_A_FLOAT_ARRAY_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"ManyFloats\",     " + "  \"fields\": [           "
      + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\" },  " + "       { \"name\": \""
      + DEFAULT_VALUE_FIELD_PROP + "\", \"type\": {\"type\": \"array\", \"items\": \"float\"} },  "
      + "       { \"name\": \"age\", \"type\": \"int\" }" + "  ] " + " } ";

  public static final String INT_STRING_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"IntToString\",     " + "  \"fields\": [           "
      + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"int\"},  " + "       { \"name\": \""
      + DEFAULT_VALUE_FIELD_PROP + "\", \"type\": \"string\"}  " + "  ] " + " } ";

  public static final String STRING_STRING_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"StringToString\",     " + "  \"fields\": [           "
      + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\"},  " + "       { \"name\": \""
      + DEFAULT_VALUE_FIELD_PROP + "\", \"type\": \"string\"}  " + "  ] " + " } ";

  public static final String NESTED_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"nameRecord\",     " + "  \"fields\": [           "
      + "       { \"name\": \"firstName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"lastName\", \"type\": \"string\", \"default\": \"\" }  " + "  ]" + " } ";
  public static final String NESTED_SCHEMA_STRING_V2 = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"nameRecord\",     " + "  \"fields\": [           "
      + "       { \"name\": \"firstName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"lastName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1 }  " + "  ]" + " } ";

  public static final String STRING_RECORD_SCHEMA_STRING =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
          + "  \"name\": \"StringToRecord\",     " + "  \"fields\": [           " + "       { \"name\": \""
          + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\", \"default\": \"\"}, " + "       { \"name\": \""
          + DEFAULT_VALUE_FIELD_PROP + "\", \"type\": " + NESTED_SCHEMA_STRING + " }  " + "  ] " + " } ";

  public static final String STRING_SCHEMA = "\"string\"";

  public static final String INT_SCHEMA = "\"int\"";

  public static final int DEFAULT_USER_DATA_RECORD_COUNT = 100;

  public static final String DEFAULT_USER_DATA_VALUE_PREFIX = "test_name_";

  public static File getTempDataDirectory() {
    return Utils.getTempDataDirectory();
  }

  /**
   * This function is used to generate a small avro file with 'user' schema.
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir) throws IOException {
    return writeSimpleAvroFileWithUserSchema(parentDir, true);
  }

  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir, int recordLength) throws IOException {
    return writeSimpleAvroFileWithUserSchema(parentDir, true, DEFAULT_USER_DATA_RECORD_COUNT, recordLength);
  }

  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    return writeSimpleAvroFileWithUserSchema(parentDir, fileNameWithAvroSuffix, DEFAULT_USER_DATA_RECORD_COUNT);
  }

  public static Schema writeSimpleAvroFileWithUserSchema(
      File parentDir,
      boolean fileNameWithAvroSuffix,
      int recordCount) throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_user.avro";
    } else {
      fileName = "simple_user";
    }
    return writeSimpleAvroFileWithUserSchema(parentDir, recordCount, fileName);
  }

  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir, int recordCount, String fileName)
      throws IOException {
    return writeAvroFile(parentDir, fileName, USER_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
        user.put("age", i);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithUserSchema(
      File parentDir,
      boolean fileNameWithAvroSuffix,
      int recordCount,
      int recordSizeMin) throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_user.avro";
    } else {
      fileName = "simple_user";
    }
    char[] chars = new char[recordSizeMin];
    return writeAvroFile(parentDir, fileName, USER_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        Arrays.fill(chars, String.valueOf(i).charAt(0));
        user.put(DEFAULT_VALUE_FIELD_PROP, String.copyValueOf(chars));
        user.put("age", i);
        writer.append(user);
      }
    });
  }

  public static void writeMultipleAvroFilesWithUserSchema(File parentDir, int fileCount, int recordCount)
      throws IOException {
    for (int i = 0; i < fileCount; i++) {
      writeSimpleAvroFileWithUserSchema(parentDir, recordCount, "testInput" + i + ".avro");
    }
  }

  public static Schema writeSimpleAvroFileForValidateSchemaAndBuildDictMapperOutput(
      File parentDir,
      String file,
      long inputFileDataSize,
      ByteBuffer zstdDictionary,
      String avroSchema) throws IOException {
    return writeAvroFile(parentDir, file, avroSchema, (recordSchema, writer) -> {
      GenericRecord user = new GenericData.Record(recordSchema);
      user.put(KEY_INPUT_FILE_DATA_SIZE, inputFileDataSize);
      if (zstdDictionary != null) {
        user.put(KEY_ZSTD_COMPRESSION_DICTIONARY, zstdDictionary);
      }
      writer.append(user);
    });
  }

  public static Schema writeETLFileWithUserSchema(File parentDir, boolean fileNameWithAvroSuffix) throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_etl_user.avro";
    } else {
      fileName = "simple_etl_user";
    }

    return writeAvroFile(
        parentDir,
        fileName,
        getETLStoreSchemaString(ETL_KEY_SCHEMA_STRING, ETL_VALUE_SCHEMA_STRING),
        (recordSchema, writer) -> {
          for (int i = 1; i <= 50; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));
            GenericRecord value = new GenericData.Record(Schema.parse(ETL_VALUE_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
            value.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", value);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 51; i <= 100; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", null);
            user.put("offset", (long) i);
            user.put("DELETED_TS", (long) i);

            writer.append(user);
          }
        });
  }

  public static Schema writeETLFileWithUnionWithNullSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_etl_union_with_null.avro";
    } else {
      fileName = "simple_etl_union_with_null";
    }

    return writeAvroFile(
        parentDir,
        fileName,
        getETLStoreSchemaString(ETL_KEY_SCHEMA_STRING, ETL_UNION_VALUE_SCHEMA_STRING_WITH_NULL),
        (recordSchema, writer) -> {
          for (int i = 1; i <= 25; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", "string_" + i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 26; i <= 50; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 51; i <= 100; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", null);
            user.put("offset", (long) i);
            user.put("DELETED_TS", (long) i);

            writer.append(user);
          }
        });
  }

  public static Schema writeETLFileWithUnionWithoutNullSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_etl_union_without_null.avro";
    } else {
      fileName = "simple_etl_union_without_null";
    }

    return writeAvroFile(
        parentDir,
        fileName,
        getETLStoreSchemaString(ETL_KEY_SCHEMA_STRING, ETL_UNION_VALUE_SCHEMA_STRING_WITHOUT_NULL),
        (recordSchema, writer) -> {
          for (int i = 1; i <= 25; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", "string_" + i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 26; i <= 50; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", i);
            user.put("offset", (long) i);
            user.put("DELETED_TS", null);

            writer.append(user);
          }

          for (int i = 51; i <= 100; ++i) {
            GenericRecord user = new GenericData.Record(recordSchema);

            GenericRecord key = new GenericData.Record(Schema.parse(ETL_KEY_SCHEMA_STRING));

            key.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));

            user.put("opalSegmentIdPart", 0);
            user.put("opalSegmentIdSeq", 0);
            user.put("opalSegmentOffset", (long) 0);
            user.put("metadata", new HashMap<>());

            user.put("key", key);
            user.put("value", null);
            user.put("offset", (long) i);
            user.put("DELETED_TS", (long) i);

            writer.append(user);
          }
        });
  }

  public static Schema writeAlternateSimpleAvroFileWithUserSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_user.avro";
    } else {
      fileName = "simple_user";
    }
    return writeAvroFile(parentDir, fileName, USER_SCHEMA_STRING, (recordSchema, writer) -> {
      String name = "alternate_test_name_";
      for (int i = 1; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, name + i);
        user.put("age", i);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithIntToStringSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    return writeSimpleAvroFileWithIntToStringSchema(parentDir, fileNameWithAvroSuffix, 100);
  }

  public static Schema writeSimpleAvroFileWithIntToStringSchema(
      File parentDir,
      boolean fileNameWithAvroSuffix,
      int recordCount) throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_int2string.avro";
    } else {
      fileName = "simple_int2string";
    }
    return writeAvroFile(parentDir, fileName, INT_STRING_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord i2i = new GenericData.Record(recordSchema);
        i2i.put(DEFAULT_KEY_FIELD_PROP, i);
        i2i.put(DEFAULT_VALUE_FIELD_PROP, "name " + Integer.toString(i));
        writer.append(i2i);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToStringSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_string2string.avro";
    } else {
      fileName = "simple_string2string";
    }
    return writeAvroFile(parentDir, fileName, STRING_STRING_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 1; i <= 100; ++i) {
        GenericRecord s2s = new GenericData.Record(recordSchema);
        s2s.put(DEFAULT_KEY_FIELD_PROP, "jobPosting:" + i);
        s2s.put(DEFAULT_VALUE_FIELD_PROP, String.valueOf(i));
        writer.append(s2s);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithStringToRecordSchema(File parentDir, boolean fileNameWithAvroSuffix)
      throws IOException {
    String fileName;
    if (fileNameWithAvroSuffix) {
      fileName = "simple_string2record.avro";
    } else {
      fileName = "simple_string2record";
    }
    return writeAvroFile(parentDir, fileName, STRING_RECORD_SCHEMA_STRING, (recordSchema, writer) -> {
      Schema valueSchema = AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING);
      String firstName = "first_name_";
      String lastName = "last_name_";
      for (int i = 1; i <= 100; ++i) {
        GenericRecord keyValueRecord = new GenericData.Record(recordSchema);
        keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, String.valueOf(i)); // Key
        GenericRecord valueRecord = new GenericData.Record(valueSchema);
        valueRecord.put("firstName", firstName + i);
        valueRecord.put("lastName", lastName + i);
        keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord); // Value
        writer.append(keyValueRecord);
      }
    });
  }

  /**
   * This file overrides half of the value in {@link #writeSimpleAvroFileWithUserSchema(File)}
   * and add some new values.
   * It's designed to test incremental push
   */
  public static Schema writeSimpleAvroFileWithUserSchema2(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "simple_user.avro", USER_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 51; i <= 150; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + (i * 2));
        user.put("age", i * 2);
        writer.append(user);
      }
    });
  }

  /**
   * This file add some new value in {@link #writeSimpleAvroFileWithUserSchema(File)}
   * It's designed to test incremental push
   */
  public static Schema writeSimpleAvroFileWithUserSchema3(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "simple_user.avro", USER_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 51; i <= 200; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + (i * 3));
        user.put("age", i * 3);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithDuplicateKey(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "duplicate_key_user.avro", USER_SCHEMA_STRING, (recordSchema, avroFileWriter) -> {
      for (int i = 0; i < 100; i++) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, i % 10 == 0 ? "0" : Integer.toString(i)); // DEFAULT_KEY_FIELD_PROP is the key
        user.put(DEFAULT_VALUE_FIELD_PROP, "test_name" + i);
        user.put("age", i);
        avroFileWriter.append(user);
      }
    });
  }

  public static void writeInvalidAvroFile(File parentDir, String fileName) throws IOException {
    PrintWriter writer = new PrintWriter(parentDir.getAbsolutePath() + "/" + fileName, "UTF-8");
    writer.println("Invalid file");
    writer.close();
  }

  public static Schema writeEmptyAvroFileWithUserSchema(File parentDir, String fileName, String schema)
      throws IOException {
    return writeAvroFile(parentDir, fileName, schema, (recordSchema, avroFileWriter) -> {
      // No-op so that the file is empty
    });
  }

  public static Schema writeEmptyAvroFileWithUserSchema(File parentDir) throws IOException {
    return writeEmptyAvroFileWithUserSchema(parentDir, "empty_file.avro", USER_SCHEMA_STRING);
  }

  public static Schema writeSimpleAvroFileWithCustomSize(
      File parentDir,
      int numberOfRecords,
      int minValueSize,
      int maxValueSize) throws IOException {
    return writeAvroFile(parentDir, "large_values.avro", USER_SCHEMA_STRING, (recordSchema, avroFileWriter) -> {
      int sizeRange = maxValueSize - minValueSize;
      for (int i = 0; i < numberOfRecords; i++) {
        int sizeForThisRecord = minValueSize + sizeRange / numberOfRecords * (i + 1);
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i)); // DEFAULT_KEY_FIELD_PROP is the key
        char[] chars = new char[sizeForThisRecord];
        Arrays.fill(chars, Integer.toString(i).charAt(0));
        Utf8 utf8Value = new Utf8(new String(chars));
        user.put(DEFAULT_VALUE_FIELD_PROP, utf8Value);
        user.put("age", i);
        avroFileWriter.append(user);
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
    return writeAvroFile(
        parentDir,
        "record_with_wrong_default.avro",
        schemaWithWrongDefaultValue,
        (recordSchema, avroFileWriter) -> {
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
        USER_SCHEMA_WITH_A_FLOAT_ARRAY_STRING,
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

  private static Schema writeAvroFile(
      File parentDir,
      String fileName,
      String recordSchemaStr,
      AvroFileWriter fileWriter) throws IOException {
    Schema recordSchema = AvroCompatibilityHelper.parse(recordSchemaStr);
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
      String storeName) {
    final String controllerServiceName;
    parentRegionName = parentRegionName == null ? "parentRegion" : parentRegionName;
    Properties props = new Properties();
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

  public static Properties defaultVPJProps(String veniceUrl, String inputDirPath, String storeName) {
    Properties props = new Properties();
    props.put(VENICE_DISCOVER_URL_PROP, veniceUrl);
    return defaultVPJPropsInternal(props, inputDirPath, storeName);
  }

  private static Properties defaultVPJPropsInternal(Properties props, String inputDirPath, String storeName) {
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(INPUT_PATH_PROP, inputDirPath);
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(POLL_JOB_STATUS_INTERVAL_MS, 1000);
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "test");
    props.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, "false");
    props.setProperty(CONTROLLER_REQUEST_RETRY_ATTEMPTS, "5");
    return props;
  }

  public static String loadFileAsString(String fileName) throws IOException {
    return IOUtils.toString(
        Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
        StandardCharsets.UTF_8);
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

  public static String getETLStoreSchemaString(String keySchema, String valueSchema) {
    String finalValueSchema =
        ETLUtils.transformValueSchemaForETL(AvroCompatibilityHelper.parse(valueSchema)).toString();
    return "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"storeName_v1\",\n"
        + "  \"namespace\": \"com.linkedin.gobblin.venice.model\",\n" + "  \"fields\": [\n" + "    {\n"
        + "      \"name\": \"opalSegmentIdPart\",\n" + "      \"type\": \"int\",\n"
        + "      \"doc\": \"Opal segment id partition\"\n" + "    },\n" + "    {\n"
        + "      \"name\": \"opalSegmentIdSeq\",\n" + "      \"type\": \"int\",\n"
        + "      \"doc\": \"Opal segment id sequence\"\n" + "    },\n" + "    {\n"
        + "      \"name\": \"opalSegmentOffset\",\n" + "      \"type\": \"long\",\n"
        + "      \"doc\": \"Opal segment offset\"\n" + "    },\n" + "    {\n" + "      \"name\": \"key\",\n"
        + "      \"type\":" + keySchema + ",\n" + "      \"doc\": \"Raw bytes of the key\"\n" + "    },\n" + "    {\n"
        + "      \"name\": \"value\",\n" + "      \"type\":" + finalValueSchema + ",\n"
        + "      \"doc\": \"Raw bytes of the value\"\n" + "    },\n" + "    {\n" + "      \"name\": \"offset\",\n"
        + "      \"type\": \"long\",\n" + "      \"doc\": \"The offset of this record in Kafka\"\n" + "    },\n"
        + "    {\n" + "      \"name\": \"DELETED_TS\",\n" + "      \"type\": [\n" + "        \"null\",\n"
        + "        \"long\"\n" + "      ],\n"
        + "      \"doc\": \"If the current record is a PUT, this field will be null; if it's a DELETE, this field will be the offset of the record in Kafka\",\n"
        + "      \"default\": null\n" + "    },\n" + "    {\n" + "      \"name\": \"metadata\",\n"
        + "      \"type\": {\n" + "        \"type\": \"map\",\n" + "        \"values\": {\n"
        + "          \"type\": \"string\",\n" + "          \"avro.java.string\": \"String\"\n" + "        },\n"
        + "        \"avro.java.string\": \"String\"\n" + "      },\n"
        + "      \"doc\": \"Metadata of the record; currently it contains the schemaId of the record\",\n"
        + "      \"default\": {}\n" + "    }\n" + "  ]\n" + "}";
  }

  public static void runPushJob(String jobId, Properties props) {
    runPushJob(jobId, props, noOp -> {});
  }

  public static void runPushJob(String jobId, Properties props, Consumer<VenicePushJob> jobTransformer) {
    try (VenicePushJob job = new VenicePushJob(jobId, props)) {
      jobTransformer.accept(job);
      job.run();
    }
  }
}
