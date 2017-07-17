package com.linkedin.venice.utils;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;


public class TestPushUtils {
  public static final String USER_SCHEMA_STRING = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\" },  " +
      "       { \"name\": \"name\", \"type\": \"string\" },  " +
      "       { \"name\": \"age\", \"type\": \"int\" }" +
      "  ] " +
      " } ";

  public static File getTempDataDirectory() {
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString("Venice-Data");
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dir.mkdir();
    dir.deleteOnExit();
    return dir;
  }

  /**
   * This function is used to generate a small avro file with 'user' schema.
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir) throws IOException {
    Schema schema = Schema.parse(USER_SCHEMA_STRING);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    for (int i = 1; i <= 100; ++i) { // DEBUG this should be 100
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
    return schema;
  }

  public static Properties defaultH2VProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Properties props = new Properties();
    props.put(KafkaPushJob.VENICE_URL_PROP, veniceCluster.getRandomRouterURL());
    props.put(KafkaPushJob.KAFKA_URL_PROP, veniceCluster.getKafka().getAddress());
    props.put(KafkaPushJob.VENICE_CLUSTER_NAME_PROP, veniceCluster.getClusterName());
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(KafkaPushJob.VENICE_STORE_OWNERS_PROP, "test@linkedin.com");
    props.put(KafkaPushJob.INPUT_PATH_PROP, inputDirPath);
    props.put(KafkaPushJob.AVRO_KEY_FIELD_PROP, "id");
    props.put(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);

    return props;
  }

  public static void createStoreForJob(VeniceClusterWrapper veniceCluster, Schema recordSchema, Properties props) {
    ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), props.getProperty(KafkaPushJob.VENICE_URL_PROP));
    Schema keySchema = recordSchema.getField(props.getProperty(KafkaPushJob.AVRO_KEY_FIELD_PROP)).schema();
    Schema valueSchema = recordSchema.getField(props.getProperty(KafkaPushJob.AVRO_VALUE_FIELD_PROP)).schema();
    controllerClient.createNewStore(props.getProperty(VENICE_STORE_NAME_PROP),
        props.getProperty(KafkaPushJob.VENICE_STORE_OWNERS_PROP), keySchema.toString(), valueSchema.toString());
    controllerClient.updateStore(props.getProperty(VENICE_STORE_NAME_PROP), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(Store.UNLIMITED_STORAGE_QUOTA), Optional.empty(),
        Optional.empty(), Optional.empty());
  }
}
