package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.hadoop.VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.hadoop.VenicePushJob.INPUT_PATH_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.POLL_JOB_STATUS_INTERVAL_MS;
import static com.linkedin.venice.hadoop.VenicePushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_URL_PROP;
import static com.linkedin.venice.meta.Version.PushType;
import static com.linkedin.venice.samza.VeniceSystemFactory.D2_ZK_HOSTS_PROPERTY;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.etl.ETLUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.io.IOException;
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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;


public class TestPushUtils {
  public static final String USER_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\"},  " + "       { \"name\": \"name\", \"type\": \"string\"},  "
      + "       { \"name\": \"age\", \"type\": \"int\" }" + "  ] " + " } ";

  public static final String ETL_KEY_SCHEMA_STRING = "{\n" + "    \"type\":\"record\",\n" + "    \"name\":\"key\",\n"
      + "    \"namespace\":\"com.linkedin.vencie.testkey\",\n" + "    \"fields\":[\n" + "        {\n"
      + "            \"name\":\"id\",\n" + "            \"type\":\"string\"\n" + "        }\n" + "    ]\n" + "}";

  public static final String ETL_VALUE_SCHEMA_STRING = "{\n" + "    \"type\":\"record\",\n"
      + "    \"name\":\"value\",\n" + "    \"namespace\":\"com.linkedin.vencie.testvalue\",\n" + "    \"fields\":[\n"
      + "        {\n" + "            \"name\":\"name\",\n" + "            \"type\":\"string\"\n" + "        }\n"
      + "    ],\n" + "    \"version\":10\n" + "}";

  public static final String ETL_UNION_VALUE_SCHEMA_STRING_WITHOUT_NULL = "[\"int\", \"string\"]";

  public static final String ETL_UNION_VALUE_SCHEMA_STRING_WITH_NULL = "[\"int\", \"string\", \"null\"]";

  public static final String USER_SCHEMA_STRING_SIMPLE_WITH_DEFAULT =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"\"}" + "  ] " + " } ";

  public static final String USER_SCHEMA_STRING_WITH_DEFAULT =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"\"},  "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": 1 }" + "  ] " + " } ";

  public static final String USER_SCHEMA_WITH_A_FLOAT_ARRAY_STRING =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"ManyFloats\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\" },  "
          + "       { \"name\": \"name\", \"type\": {\"type\": \"array\", \"items\": \"float\"} },  "
          + "       { \"name\": \"age\", \"type\": \"int\" }" + "  ] " + " } ";

  public static final String INT_STRING_SCHEMA_STRING =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"IntToString\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"int\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\"}  " + "  ] " + " } ";

  public static final String STRING_STRING_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"StringToString\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\"},  " + "       { \"name\": \"name\", \"type\": \"string\"}  "
      + "  ] " + " } ";

  public static final String NESTED_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"nameRecord\",     " + "  \"fields\": [           "
      + "       { \"name\": \"firstName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"lastName\", \"type\": \"string\", \"default\": \"\" }  " + "  ]" + " } ";
  public static final String NESTED_SCHEMA_STRING_V2 = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"nameRecord\",     " + "  \"fields\": [           "
      + "       { \"name\": \"firstName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"lastName\", \"type\": \"string\", \"default\": \"\" },  "
      + "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1 }  " + "  ]" + " } ";

  public static final String STRING_RECORD_SCHEMA_STRING = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"StringToRecord\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}, "
      + "       { \"name\": \"name\", \"type\": " + NESTED_SCHEMA_STRING + " }  " + "  ] " + " } ";

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
    return writeAvroFile(parentDir, fileName, USER_SCHEMA_STRING, (recordSchema, writer) -> {
      for (int i = 1; i <= recordCount; ++i) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put("id", Integer.toString(i));
        user.put("name", DEFAULT_USER_DATA_VALUE_PREFIX + i);
        user.put("age", i);
        writer.append(user);
      }
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

            key.put("id", Integer.toString(i));
            value.put("name", DEFAULT_USER_DATA_VALUE_PREFIX + i);

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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

            key.put("id", Integer.toString(i));

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
        user.put("id", Integer.toString(i));
        user.put("name", name + i);
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
        i2i.put("id", i);
        i2i.put("name", "name " + Integer.toString(i));
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
        s2s.put("id", "jobPosting:" + i);
        s2s.put("name", String.valueOf(i));
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
        keyValueRecord.put("id", String.valueOf(i)); // Key
        GenericRecord valueRecord = new GenericData.Record(valueSchema);
        valueRecord.put("firstName", firstName + i);
        valueRecord.put("lastName", lastName + i);
        keyValueRecord.put("name", valueRecord); // Value
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
        user.put("id", Integer.toString(i));
        user.put("name", DEFAULT_USER_DATA_VALUE_PREFIX + (i * 2));
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
        user.put("id", Integer.toString(i));
        user.put("name", DEFAULT_USER_DATA_VALUE_PREFIX + (i * 3));
        user.put("age", i * 3);
        writer.append(user);
      }
    });
  }

  public static Schema writeSimpleAvroFileWithDuplicateKey(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "duplicate_key_user.avro", USER_SCHEMA_STRING, (recordSchema, avroFileWriter) -> {
      for (int i = 0; i < 100; i++) {
        GenericRecord user = new GenericData.Record(recordSchema);
        user.put("id", i % 10 == 0 ? "0" : Integer.toString(i)); // "id" is the key
        user.put("name", "test_name" + i);
        user.put("age", i);
        avroFileWriter.append(user);
      }
    });
  }

  public static Schema writeEmptyAvroFileWithUserSchema(File parentDir) throws IOException {
    return writeAvroFile(parentDir, "empty_file.avro", USER_SCHEMA_STRING, (recordSchema, avroFileWriter) -> {
      // No-op so that the file is empty
    });
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
        user.put("id", Integer.toString(i)); // "id" is the key
        char[] chars = new char[sizeForThisRecord];
        Arrays.fill(chars, Integer.toString(i).charAt(0));
        Utf8 utf8Value = new Utf8(new String(chars));
        user.put("name", utf8Value);
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
        + "        \"fields\": [\n" + "         {\"name\": \"id\", \"type\": \"string\"},\n"
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
            value.put("id", Integer.toString(i));
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
    user.put("id", Integer.toString(index)); // "id" is the key
    int numberOfFloats = size / 4;
    List<Float> floatsArray = new ArrayList<>();
    for (int j = 0; j < numberOfFloats; j++) {
      floatsArray.add(RandomGenUtils.getRandomFloat());
    }
    user.put("name", floatsArray);
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

  public static Pair<Schema, Schema> writeSimpleVsonFile(File parentDir) throws IOException {
    String vsonInteger = "\"int32\"";
    String vsonString = "\"string\"";

    writeVsonFile(vsonInteger, vsonString, parentDir, "simple_vson_file", (keySerializer, valueSerializer, writer) -> {
      for (int i = 0; i < 100; i++) {
        writer.append(
            new BytesWritable(keySerializer.toBytes(i)),
            new BytesWritable(valueSerializer.toBytes(String.valueOf(i + 100))));
      }
    });
    return new Pair<>(VsonAvroSchemaAdapter.parse(vsonInteger), VsonAvroSchemaAdapter.parse(vsonString));
  }

  public enum testRecordType {
    NEARLINE, OFFLINE;
  }

  public enum testTargetedField {
    WEBSITE_URL, LOGO, INDUSTRY;
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
              .put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, testRecordType.NEARLINE.toString()));
        } else {
          keyRecord
              .put("source", AvroCompatibilityHelper.newEnumSymbol(sourceSchema, testRecordType.OFFLINE.toString()));
        }

        GenericRecord valueRecord = new GenericData.Record(schemaWithSymbolDoc.getField("value").schema());
        Schema targetSchema = valueRecord.getSchema().getField("targetedField").schema();
        valueRecord.put("priority", i);
        if (i % 3 == 0) {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, testTargetedField.WEBSITE_URL.toString()));
        } else if (i % 3 == 1) {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, testTargetedField.LOGO.toString()));
        } else {
          valueRecord.put(
              "targetedField",
              AvroCompatibilityHelper.newEnumSymbol(targetSchema, testTargetedField.INDUSTRY.toString()));
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
  public static Pair<Schema, Schema> writeVsonByteAndShort(File parentDir) throws IOException {
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
    return new Pair<>(VsonAvroSchemaAdapter.parse(vsonByte), VsonAvroSchemaAdapter.parse(vsonShort));
  }

  public static Pair<Schema, Schema> writeComplexVsonFile(File parentDir) throws IOException {
    String vsonInteger = "\"int32\"";
    String vsonString = "{\"member_id\":\"int32\", \"score\":\"float32\"}";
    ;

    Map<String, Object> record = new HashMap<>();
    writeVsonFile(vsonInteger, vsonString, parentDir, "complex_vson-file", (keySerializer, valueSerializer, writer) -> {
      for (int i = 0; i < 100; i++) {
        record.put("member_id", i + 100);
        record.put("score", i % 10 != 0 ? (float) i : null); // allow to have optional field
        writer.append(new BytesWritable(keySerializer.toBytes(i)), new BytesWritable(valueSerializer.toBytes(record)));
      }
    });
    return new Pair<>(VsonAvroSchemaAdapter.parse(vsonInteger), VsonAvroSchemaAdapter.parse(vsonString));
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

  public static Pair<Schema, Schema> writeMultiLevelVsonFile(File parentDir) throws IOException {
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
    return new Pair<>(VsonAvroSchemaAdapter.parse(vsonKeyStr), VsonAvroSchemaAdapter.parse(vsonValueStr));
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

  private static Pair<Schema, Schema> writeVsonFile(
      String keySchemaStr,
      String valueSchemStr,
      File parentDir,
      String fileName,
      VsonFileWriter fileWriter) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(new Text("key.schema"), new Text(keySchemaStr));
    metadata.set(new Text("value.schema"), new Text(valueSchemStr));

    VsonAvroSerializer keySerializer = VsonAvroSerializer.fromSchemaStr(keySchemaStr);
    VsonAvroSerializer valueSerializer = VsonAvroSerializer.fromSchemaStr(valueSchemStr);

    try (SequenceFile.Writer writer = SequenceFile.createWriter(
        new Configuration(),
        SequenceFile.Writer.file(new Path(parentDir.toString(), fileName)),
        SequenceFile.Writer.keyClass(BytesWritable.class),
        SequenceFile.Writer.valueClass(BytesWritable.class),
        SequenceFile.Writer.metadata(metadata))) {
      fileWriter.write(keySerializer, valueSerializer, writer);
    }
    return new Pair<>(VsonAvroSchemaAdapter.parse(keySchemaStr), VsonAvroSchemaAdapter.parse(valueSchemStr));
  }

  private interface VsonFileWriter {
    void write(VsonAvroSerializer keySerializer, VsonAvroSerializer valueSerializer, SequenceFile.Writer writer)
        throws IOException;
  }

  private interface AvroFileWriter {
    void write(Schema recordSchema, DataFileWriter writer) throws IOException;
  }

  public static Properties defaultVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    return defaultVPJProps(veniceCluster.getRandmonVeniceController().getControllerUrl(), inputDirPath, storeName);
  }

  public static Properties defaultVPJProps(String veniceUrl, String inputDirPath, String storeName) {
    Properties props = new Properties();
    props.put(VENICE_URL_PROP, veniceUrl);
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(INPUT_PATH_PROP, inputDirPath);
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
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

  public static Properties sslVPJProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.putAll(KafkaSSLUtils.getLocalKafkaClientSSLConfig());
    // remove the path for certs and pwd, because we will get them from hadoop user credentials.
    props.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    props.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    props.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    props.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    return props;
  }

  public static ControllerClient createStoreForJob(String veniceClusterName, Schema recordSchema, Properties props) {
    String keySchemaStr = recordSchema.getField(props.getProperty(KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VALUE_FIELD_PROP)).schema().toString();

    return createStoreForJob(
        veniceClusterName,
        keySchemaStr,
        valueSchemaStr,
        props,
        CompressionStrategy.NO_OP,
        false,
        false);
  }

  public static ControllerClient createStoreForJob(
      VeniceClusterWrapper veniceClusterWrapper,
      String keySchemaStr,
      String valueSchema,
      Properties props) {
    return createStoreForJob(veniceClusterWrapper, keySchemaStr, valueSchema, props, CompressionStrategy.NO_OP, false);
  }

  public static ControllerClient createStoreForJob(
      VeniceClusterWrapper veniceCluster,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      CompressionStrategy compressionStrategy,
      boolean chunkingEnabled) {
    return createStoreForJob(
        veniceCluster.getClusterName(),
        keySchemaStr,
        valueSchemaStr,
        props,
        compressionStrategy,
        chunkingEnabled,
        false);
  }

  public static ControllerClient createStoreForJob(
      String veniceClusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      CompressionStrategy compressionStrategy,
      boolean chunkingEnabled,
      boolean incrementalPushEnabled) {

    UpdateStoreQueryParams storeParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setCompressionStrategy(compressionStrategy)
            .setBatchGetLimit(2000)
            .setReadQuotaInCU(DEFAULT_PER_ROUTER_READ_QUOTA)
            .setChunkingEnabled(chunkingEnabled)
            .setIncrementalPushEnabled(incrementalPushEnabled);

    return createStoreForJob(veniceClusterName, keySchemaStr, valueSchemaStr, props, storeParams, false);
  }

  public static ControllerClient createStoreForJob(
      String veniceClusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      UpdateStoreQueryParams storeParams) {
    return createStoreForJob(veniceClusterName, keySchemaStr, valueSchemaStr, props, storeParams, false);
  }

  public static ControllerClient createStoreForJob(
      String veniceClusterName,
      String keySchemaStr,
      String valueSchemaStr,
      Properties props,
      UpdateStoreQueryParams storeParams,
      boolean addDerivedSchemaToStore) {

    ControllerClient controllerClient = getControllerClient(veniceClusterName, props);
    NewStoreResponse newStoreResponse = controllerClient.retryableRequest(
        5,
        c -> c.createNewStore(
            props.getProperty(VENICE_STORE_NAME_PROP),
            "test@linkedin.com",
            keySchemaStr,
            valueSchemaStr));

    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    if (addDerivedSchemaToStore) {
      // Generate write compute schema
      Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance()
          .convertFromValueRecordSchema(AvroCompatibilityHelper.parse(valueSchemaStr));
      SchemaResponse derivedValueSchemaResponse = controllerClient.retryableRequest(
          5,
          c -> c.addDerivedSchema(props.getProperty(VENICE_STORE_NAME_PROP), +1, writeComputeSchema.toString()));

      Assert.assertFalse(
          derivedValueSchemaResponse.isError(),
          "The DerivedValueSchemaResponse returned an error: " + derivedValueSchemaResponse.getError());
    }

    updateStore(veniceClusterName, props, storeParams.setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    return controllerClient;
  }

  public static void deleteStore(String veniceClusterName, Properties props) {
    try (ControllerClient controllerClient = getControllerClient(veniceClusterName, props)) {
      TestUtils.assertCommand(
          controllerClient.retryableRequest(5, c -> c.deleteStore(props.getProperty(VENICE_STORE_NAME_PROP))),
          "The delete store request returned an error");
    }
  }

  public static void disableStore(String veniceClusterName, Properties props) {
    try (ControllerClient controllerClient = getControllerClient(veniceClusterName, props)) {
      TestUtils.assertCommand(
          controllerClient
              .retryableRequest(5, c -> c.enableStoreReadWrites(props.getProperty(VENICE_STORE_NAME_PROP), false)),
          "The disable store request returned an error");
    }
  }

  public static void updateStore(String veniceClusterName, Properties props, UpdateStoreQueryParams params) {
    try (ControllerClient controllerClient = getControllerClient(veniceClusterName, props)) {
      ControllerResponse updateStoreResponse =
          controllerClient.retryableRequest(5, c -> c.updateStore(props.getProperty(VENICE_STORE_NAME_PROP), params));

      Assert.assertFalse(
          updateStoreResponse.isError(),
          "The UpdateStore response returned an error: " + updateStoreResponse.getError());
    }
  }

  private static ControllerClient getControllerClient(String veniceClusterName, Properties props) {
    String veniceUrl = props.containsKey(VENICE_DISCOVER_URL_PROP)
        ? props.getProperty(VENICE_DISCOVER_URL_PROP)
        : props.getProperty(VENICE_URL_PROP);
    return ControllerClient.constructClusterControllerClient(veniceClusterName, veniceUrl);
  }

  public static void makeStoreLF(VeniceClusterWrapper venice, String storeName) {
    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(venice.getClusterName(), venice.getRandomRouterURL())) {
      ControllerResponse response =
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    }
  }

  public static void makeStoreHybrid(
      VeniceClusterWrapper venice,
      String storeName,
      long rewindSeconds,
      long offsetLag) {
    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(venice.getClusterName(), venice.getRandomRouterURL())) {
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(rewindSeconds).setHybridOffsetLagThreshold(offsetLag));
      if (response.isError()) {
        throw new VeniceException(response.getError());
      }
    }
  }

  public static Map<String, String> getSamzaProducerConfig(
      VeniceClusterWrapper venice,
      String storeName,
      PushType type) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, type.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(D2_ZK_HOSTS_PROPERTY, venice.getZk().getAddress());
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_zk_address");
    samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    return samzaConfig;
  }

  @SafeVarargs
  public static SystemProducer getSamzaProducer(
      VeniceClusterWrapper venice,
      String storeName,
      PushType type,
      Pair<String, String>... optionalConfigs) {
    Map<String, String> samzaConfig = getSamzaProducerConfig(venice, storeName, type);
    for (Pair<String, String> config: optionalConfigs) {
      samzaConfig.put(config.getFirst(), config.getSecond());
    }
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    veniceProducer.start();
    return veniceProducer;
  }

  /**
   * Generate a streaming record using the provided producer to the specified store
   * key and value schema of the store must both be "string", the record produced is
   * based on the provided recordId
   */
  public static void sendStreamingRecord(SystemProducer producer, String storeName, int recordId) {
    sendStreamingRecord(producer, storeName, Integer.toString(recordId), "stream_" + recordId);
  }

  public static void sendStreamingRecordWithKeyPrefix(
      SystemProducer producer,
      String storeName,
      String keyPrefix,
      int recordId) {
    sendStreamingRecord(producer, storeName, keyPrefix + recordId, "stream_" + recordId);
  }

  public static void sendStreamingDeleteRecord(SystemProducer producer, String storeName, String key) {
    sendStreamingRecord(producer, storeName, key, null);
  }

  public static void sendStreamingRecord(SystemProducer producer, String storeName, Object key, Object message) {
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(new SystemStream("venice", storeName), key, message);
    producer.send(storeName, envelope);
    producer.flush(storeName);
  }

  /**
   * Identical to {@link #sendStreamingRecord(SystemProducer, String, int)} except that the value's length is equal
   * to {@param valueSizeInBytes}. The value is composed exclusively of the first digit of the {@param recordId}.
   *
   * @see #sendStreamingRecord(SystemProducer, String, int)
   */
  public static void sendCustomSizeStreamingRecord(
      SystemProducer producer,
      String storeName,
      int recordId,
      int valueSizeInBytes) {
    char[] chars = new char[valueSizeInBytes];
    Arrays.fill(chars, Integer.toString(recordId).charAt(0));
    sendStreamingRecord(producer, storeName, Integer.toString(recordId), new String(chars));
  }

  public static String loadFileAsString(String fileName) throws IOException {
    return IOUtils.toString(
        Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
        StandardCharsets.UTF_8);
  }

  public static void updateStore(
      String clusterName,
      String storeName,
      ControllerClient controllerClient,
      UpdateStoreQueryParams params) {
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
