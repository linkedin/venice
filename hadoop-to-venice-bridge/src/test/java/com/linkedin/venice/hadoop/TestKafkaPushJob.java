package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroStoreClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;

//TODO: we shall probably move it to integration test
public class TestKafkaPushJob {
  private static final Logger LOGGER = Logger.getLogger(TestKafkaPushJob.class);
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private static final String STRING_SCHEMA = "\"string\"";

  private static List<File> tempDirectories = Collections.synchronizedList(new ArrayList<File>());

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

  /* This leaves temp directories lying around, they are cleaned up in the cleanUp() method */
  protected static File getTempDataDirectory() {
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString("Venice-Data");
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dir.mkdir();
    tempDirectories.add(dir);
    return dir;
  }

  /**
   * This function is used to generate a small avro file with 'user' schema.
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  protected static Schema writeSimpleAvroFileWithUserSchema(File parentDir) throws IOException {
    String schemaStr = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" }" +
        "  ] " +
        " } ";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    for (int i = 1; i <= 100; ++i) {
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
    return schema;
  }

  /**
   *
   * @param parentDir
   * @param addFieldWithDefaultValue
   * @return the Schema object for the avro file
   * @throws IOException
   */
  protected static Schema writeComplicatedAvroFileWithUserSchema(File parentDir, boolean addFieldWithDefaultValue) throws IOException {
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
        "              {\"name\": \"favorite_number\", \"type\": \"int\"}\n";
    if (addFieldWithDefaultValue) {
      schemaStr += ",{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
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
      if (addFieldWithDefaultValue) {
        oldValue.put("favorite_color", "red");
      }
      user.put("value", oldValue);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
    return schema;
  }

  /**
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  protected static Schema writeSimpleAvroFileWithDifferentUserSchema(File parentDir) throws IOException {
    String schemaStr = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" },  " +
        "       { \"name\": \"company\", \"type\": \"string\" }  " +
        "  ] " +
        " } ";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user_with_different_schema.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    String company = "company_";
    for (int i = 1; i <= 100; ++i) {
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      user.put("company", company + i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
    return schema;
  }

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    veniceCluster = ServiceFactory.getVeniceCluster(true); //Now with SSL!
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL());
  }

  @AfterClass
  public void cleanUp() {
    if (controllerClient != null) {
      controllerClient.close();
    }

    if (veniceCluster != null) {
      veniceCluster.close();
    }

    for (File dir: tempDirectories){
      try {
        if (dir != null){
          FileUtils.deleteDirectory(dir);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private Properties setupDefaultProps(String inputDirPath, String storeName) {
    Properties props = new Properties();
    props.put(VENICE_URL_PROP, veniceCluster.getRandomRouterURL());
    props.put(KAFKA_URL_PROP, veniceCluster.getKafka().getAddress());
    props.put(VENICE_CLUSTER_NAME_PROP, veniceCluster.getClusterName());
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(VENICE_STORE_OWNERS_PROP, "test@linkedin.com");
    props.put(INPUT_PATH_PROP, inputDirPath);
    props.put(AVRO_KEY_FIELD_PROP, "id");
    props.put(AVRO_VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);

    return props;
  }

  private void createStoreForJob(Schema recordSchema, Properties props) {
    Schema keySchema = recordSchema.getField(props.getProperty(AVRO_KEY_FIELD_PROP)).schema();
    Schema valueSchema = recordSchema.getField(props.getProperty(AVRO_VALUE_FIELD_PROP)).schema();
    controllerClient.createNewStore(props.getProperty(VENICE_STORE_NAME_PROP),
        props.getProperty(VENICE_STORE_OWNERS_PROP), keySchema.toString(), valueSchema.toString());

    updateStoreStorageQuota(props, Store.UNLIMITED_STORAGE_QUOTA);
  }

  private void updateStoreStorageQuota(Properties props, long quota) {
    controllerClient.updateStore(props.getProperty(VENICE_STORE_NAME_PROP), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(quota), Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMRJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMapOnlyJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(true);
  }

  private void testRunPushJobAndPBNJ(boolean mapOnly) throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = setupDefaultProps(inputDirPath, storeName);
    if (mapOnly) {
      props.setProperty(VENICE_MAP_ONLY, "true");
    }
    props.setProperty(PBNJ_ENABLE, "true");
    props.setProperty(PBNJ_ROUTER_URL_PROP, veniceCluster.getRandomRouterURL());
    createStoreForJob(recordSchema, props);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    String routerUrl = veniceCluster.getRandomRouterURL();
    try(AvroGenericStoreClient<String, Object> client = AvroStoreClientFactory.getAndStartAvroGenericStoreClient(routerUrl, storeName)) {
      for (int i = 1; i <= 100; ++i) {
        String expected = "test_name_" + i;
        String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
        Assert.assertEquals(actual, expected);
      }

      JobStatusQueryResponse jobStatus = ControllerClient.queryJobStatus(routerUrl, veniceCluster.getClusterName(), job.getKafkaTopic());
      Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
          "After job is complete, status should reflect that");
      // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
      // that most of the progress has completed
      Assert.assertTrue(jobStatus.getMessagesConsumed()*1.5 > jobStatus.getMessagesAvailable(),
          "Complete job should have progress");
    }
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceInconsistentSchemaException.class)
  public void testRunJobWithInputHavingDifferentSchema() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    writeSimpleAvroFileWithDifferentUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = setupDefaultProps(inputDirPath, storeName);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*key field: id1 is not found.*")
  public void testRunJobWithInvalidKeyField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Setup job properties
    String storeName = TestUtils.getUniqueString("store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath, storeName);
    // Override with not-existing key field
    props.put(AVRO_KEY_FIELD_PROP, "id1");

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   *This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceSchemaFieldNotFoundException.class,
      expectedExceptionsMessageRegExp = ".*value field: name1 is not found.*")
  public void testRunJobWithInvalidValueField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = setupDefaultProps(inputDirPath, storeName);
    // Override with not-existing value field
    props.put(AVRO_VALUE_FIELD_PROP, "name1");

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*should not have sub directory: sub-dir.*")
  public void testRunJobWithSubDirInInputDir() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Create sub directory
    File subDir = new File(inputDir, "sub-dir");
    subDir.mkdir();

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = setupDefaultProps(inputDirPath, storeName);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a unit test, doesn't require a context with a cluster.
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobByPickingUpLatestFolder() throws Exception {
    File inputDir = getTempDataDirectory();
    // Create two folders, and the latest folder with the input data file
    File oldFolder = new File(inputDir, "v1");
    oldFolder.mkdir();
    File newFolder = new File(inputDir, "v2");
    newFolder.mkdir();
    writeSimpleAvroFileWithUserSchema(newFolder);
    String inputDirPath = "file:" + inputDir.getAbsolutePath() + "/#LATEST";

    FileSystem fs = FileSystem.get(new Configuration());
    Path sourcePath = getLatestPathOfInputDirectory(inputDirPath, fs);
    Assert.assertEquals(sourcePath.toString(), "file:" + newFolder.getAbsolutePath(),
        "KafkaPushJob should parse /#LATEST to latest directory");
  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*Key schema mis-match for store.*")
  public void testRunJobWithDifferentKeySchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = TestUtils.getUniqueString("store");
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath, storeName);
    createStoreForJob(recordSchema, props);
    String jobName = "Test push job";

    // Run job with different key schema (from 'string' to 'int')
    props.setProperty(AVRO_KEY_FIELD_PROP, "age");
    KafkaPushJob job = new KafkaPushJob(jobName, props);
    job.run();
  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*Fail to validate value schema.*")
  public void testRunJobMultipleTimesWithInCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = setupDefaultProps(inputDirPath, storeName);
    createStoreForJob(recordSchema, props);
    String jobName = "Test push job";
    // Run job with different value schema (from 'string' to 'int')
    props.setProperty(AVRO_VALUE_FIELD_PROP, "age");
    KafkaPushJob job = new KafkaPushJob(jobName, props);
    job.run();
  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobMultipleTimesWithCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeComplicatedAvroFileWithUserSchema(inputDir, false);
    Schema keySchema = recordSchema.getField("id").schema();
    Schema valueSchema = recordSchema.getField("value").schema();
    String storeName = TestUtils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    // Create store with value schema
    controllerClient.createNewStore(storeName, "owner", keySchema.toString(), valueSchema.toString());

    // Upload new value
    inputDir = getTempDataDirectory();
    Schema newSchema = writeComplicatedAvroFileWithUserSchema(inputDir, true);
    String newValueSchemaString = newSchema.getField("value").schema().toString();
    controllerClient.addValueSchema(storeName, newValueSchemaString);
  }

  // This test currently only validates we can do a batch push on a hybrid store. Further hybrid tests to come!
  @Test
  public void testHybridEndToEnd() throws Exception {

    String storeName = TestUtils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    Properties h2vProperties = setupDefaultProps(inputDirPath, storeName);

    //Create store and make it a hybrid store
    createStoreForJob(recordSchema, h2vProperties);
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL());
    controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(100L), Optional.of(100L)); // 100s rewind, 100 message lag

    //Do an H2V push
    String jobName = TestUtils.getUniqueString("hybrid-job");
    KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties);
    job.run();

    //Verify some records
    AvroGenericStoreClient client =
        AvroStoreClientFactory.getAndStartAvroGenericStoreClient(veniceCluster.getRandomRouterURL(), storeName);
    for (int i = 1; i < 10; i++) {
      String key = Integer.toString(i);
      Assert.assertEquals(client.get(key).get().toString(), "test_name_" + key);
    }
  }
}
