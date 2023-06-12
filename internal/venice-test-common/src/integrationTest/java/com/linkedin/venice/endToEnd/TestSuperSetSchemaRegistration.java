package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
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


public class TestSuperSetSchemaRegistration {
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper veniceCluster;

  /**
   * @param parentDir
   * @param addFieldWithDefaultValue1
   * @param addFieldWithDefaultValue2
   * @return
   * @throws IOException
   */
  private static Schema writeComplicatedAvroFileWithUserSchema(
      File parentDir,
      boolean addFieldWithDefaultValue1,
      boolean addFieldWithDefaultValue2) throws IOException {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "      { \"name\": \"" + DEFAULT_KEY_FIELD_PROP + "\", \"type\": \"string\"},\n"
        + "      {\n" + "       \"name\": \"value\",\n" + "       \"type\": {\n" + "           \"type\": \"record\",\n"
        + "           \"name\": \"ValueRecord\",\n" + "           \"fields\" : [\n"
        + "              {\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";
    if (addFieldWithDefaultValue1) {
      schemaStr += ",{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    }
    if (addFieldWithDefaultValue2) {
      schemaStr += ",{\"name\": \"favorite_food\", \"type\": \"string\", \"default\": \"chinese\"}\n";
    }
    schemaStr += "           ]\n" + "        }\n" + "      }\n" + " ]\n" + "}";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, file);

      for (int i = 1; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(schema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        GenericRecord oldValue = new GenericData.Record(schema.getField("value").schema());
        oldValue.put("favorite_number", i);
        if (addFieldWithDefaultValue1) {
          oldValue.put("favorite_color", "red");
        }
        if (addFieldWithDefaultValue2) {
          oldValue.put("favorite_food", "italian");
        }
        user.put(DEFAULT_VALUE_FIELD_PROP, oldValue);
        dataFileWriter.append(user);
      }

      return schema;
    }
  }

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(); // Now with SSL!
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRegisterSuperSetSchemaAndPush() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    String storeName = Utils.getUniqueString("store");

    Schema recordSchema = writeComplicatedAvroFileWithUserSchema(inputDir, false, true);
    Schema keySchema = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema();
    Schema valueSchema = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema();

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    try (ControllerClient controllerClient =
        createStoreForJob(veniceCluster, keySchema.toString(), valueSchema.toString(), props)) {
      // set up superset schema gen configs
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setReadComputationEnabled(true);
      params.setAutoSchemaPushJobEnabled(true);
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
    }

    writeComplicatedAvroFileWithUserSchema(inputDir, true, true);
    props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    try (VenicePushJob job = new VenicePushJob("Test Batch push job # 1", props)) {
      job.run();
    }

    writeComplicatedAvroFileWithUserSchema(inputDir, false, false);
    props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    try (VenicePushJob job = new VenicePushJob("Test Batch push job # 2", props)) {
      job.run();
    }
  }
}
