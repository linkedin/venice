package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class TestSuperSetSchemaRegistration {
    private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
    private VeniceClusterWrapper veniceCluster;
    private ControllerClient controllerClient;

  /**
   * @param parentDir
   * @param addFieldWithDefaultValue1
   * @param addFieldWithDefaultValue2
   * @return
   * @throws IOException
   */
    private static Schema writeComplicatedAvroFileWithUserSchema(File parentDir, boolean addFieldWithDefaultValue1, boolean addFieldWithDefaultValue2) throws IOException {
      String schemaStr = "{\"namespace\": \"example.avro\",\n" +
          " \"type\": \"record\",\n" +
          " \"name\": \"User\",\n" +
          " \"fields\": [\n" +
          "      { \"name\": \"id\", \"type\": \"string\"},\n" +
          "      {\n" +
          "       \"name\": \"value\",\n" +
          "       \"type\": {\n" +
          "           \"type\": \"record\",\n" +
          "           \"name\": \"ValueRecord\",\n" +
          "           \"fields\" : [\n" +
          "              {\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";
      if (addFieldWithDefaultValue1) {
        schemaStr += ",{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
      }
      if (addFieldWithDefaultValue2) {
        schemaStr += ",{\"name\": \"favorite_food\", \"type\": \"string\", \"default\": \"chinese\"}\n";
      }
      schemaStr +=
          "           ]\n" +
              "        }\n" +
              "      }\n" +
              " ]\n" +
              "}";
      Schema schema = Schema.parse(schemaStr);
      File file = new File(parentDir, "simple_user.avro");
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
      dataFileWriter.create(schema, file);

      String name = "test_name_";
      for (int i = 1; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(schema);
        user.put("id", Integer.toString(i));
        GenericRecord oldValue = new GenericData.Record(schema.getField("value").schema());
        oldValue.put("favorite_number", i);
        if (addFieldWithDefaultValue1) {
          oldValue.put("favorite_color", "red");
        }
        if (addFieldWithDefaultValue2) {
          oldValue.put("favorite_food", "italian");
        }
        user.put("value", oldValue);
        dataFileWriter.append(user);
      }

      dataFileWriter.close();
      return schema;
    }

    @BeforeClass
    public void setUp() {
      Utils.thisIsLocalhost();
      veniceCluster = ServiceFactory.getVeniceCluster(); //Now with SSL!
      controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL());
    }

    @AfterClass
    public void cleanUp() {
      IOUtils.closeQuietly(controllerClient);
      IOUtils.closeQuietly(veniceCluster);
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testRegisterSuperSetSchemaAndPush() throws Exception {
      File inputDir = TestUtils.getTempDataDirectory();
      String storeName = TestUtils.getUniqueString("store");

      Schema recordSchema = writeComplicatedAvroFileWithUserSchema(inputDir, false, true);
      Schema keySchema = recordSchema.getField("id").schema();
      Schema valueSchema = recordSchema.getField("value").schema();

      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
      props.setProperty(VALUE_FIELD_PROP, "value");

      try (ControllerClient controllerClient = createStoreForJob(veniceCluster, keySchema.toString(), valueSchema.toString(), props)) {}

      // set up superset schema gen configs
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setReadComputationEnabled(true);
      params.setAutoSchemaPushJobEnabled(true);
      veniceCluster.updateStore(storeName, params);

      writeComplicatedAvroFileWithUserSchema(inputDir, true, true);
      props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

      props.setProperty(VALUE_FIELD_PROP, "value");

      try (KafkaPushJob job = new KafkaPushJob("Test Batch push job # 1", props)) {
        job.run();
      }

      writeComplicatedAvroFileWithUserSchema(inputDir, false, false);
      props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
      props.setProperty(VALUE_FIELD_PROP, "value");

      try (KafkaPushJob job = new KafkaPushJob("Test Batch push job # 2", props)) {
        job.run();
      }
    }
}

