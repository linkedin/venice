package com.linkedin.venice.hadoop;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestPushUtils;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


//TODO: we shall probably move it to integration test
public class TestKafkaPushJob {
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper veniceCluster;
  private ControllerClient controllerClient;

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
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Inconsistent file.* schema found.*")
  public void testRunJobWithInputHavingDifferentSchema() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    writeSimpleAvroFileWithDifferentUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

    TestPushUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*Could not find field: id1.*")
  public void testRunJobWithInvalidKeyField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Setup job properties
    String storeName = TestUtils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing key field
    props.put(KEY_FIELD_PROP, "id1");

    TestPushUtils.runPushJob("Test push job", props);

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   *This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceSchemaFieldNotFoundException.class,
      expectedExceptionsMessageRegExp = ".*Could not find field: name1.*")
  public void testRunJobWithInvalidValueField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing value field
    props.put(VALUE_FIELD_PROP, "name1");

    TestPushUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   *This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceSchemaFieldNotFoundException.class,
      expectedExceptionsMessageRegExp = ".*Could not find field: name1.*")
  public void testRunJobWithInvalidValueFieldVson() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleVsonFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing value field
    props.put(KEY_FIELD_PROP, "");
    props.put(VALUE_FIELD_PROP, "name1");

    TestPushUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*should not have sub directory.*")
  public void testRunJobWithSubDirInInputDir() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Create sub directory
    File subDir = new File(inputDir, "sub-dir");
    subDir.mkdir();

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

    TestPushUtils.runPushJob("Test push job", props);

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
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster, recordSchema, props).close();
    String jobName = "Test push job";

    // Run job with different key schema (from 'string' to 'int')
    props.setProperty(KEY_FIELD_PROP, "age");
    TestPushUtils.runPushJob("Test push job", props);

  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*Failed to validate value schema.*")
  public void testRunJobMultipleTimesWithInCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster, recordSchema, props).close();
    String jobName = "Test push job";
    // Run job with different value schema (from 'string' to 'int')
    props.setProperty(VALUE_FIELD_PROP, "age");
    props.setProperty(CONTROLLER_REQUEST_RETRY_ATTEMPTS, "2");
    TestPushUtils.runPushJob("Test push job", props);

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

  /**
   * Testing write compute enabled job where the store does not write compute enabled
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Store does not have write compute enabled.*")
  public void testWCJobWithStoreNotWCEnabled() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = TestUtils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();

    //disable WriteCompute in store
    params.setWriteComputationEnabled(false);
    params.setLeaderFollowerModel(true);
    params.setIncrementalPushEnabled(true);

    controllerClient.createNewStoreWithParameters(storeName, "owner", "\"string\"", "\"string\"", params, "random_id",
        10000);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

    //enable write compute param
    props.put(ENABLE_WRITE_COMPUTE, true);
    props.put(INCREMENTAL_PUSH, true);

    TestPushUtils.runPushJob("Test push job", props);

  }

  /**
   * Testing write compute enabled job where the store does not have LeaderFollower enabled
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Write compute is only available for incremental push jobs.*")
  public void testWCBatchJob() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = TestUtils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setWriteComputationEnabled(true);
    params.setLeaderFollowerModel(true);
    params.setIncrementalPushEnabled(false);

    controllerClient.createNewStoreWithParameters(storeName, "owner", "\"string\"", "\"string\"", params, "random_id",
        10000);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

    //enable write compute param
    props.put(ENABLE_WRITE_COMPUTE, true);
    props.put(INCREMENTAL_PUSH, false);

    TestPushUtils.runPushJob("Test push job", props);

  }

  /**
   * Testing write compute enabled job where the store does not have LeaderFollower enabled
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Leader follower mode needs to be enabled for write compute.*")
  public void testWCJobWithNoLF() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = TestUtils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setWriteComputationEnabled(true);
    params.setLeaderFollowerModel(false);     //disable LeaderFollower in store
    params.setIncrementalPushEnabled(true);

    controllerClient.createNewStoreWithParameters(storeName, "owner", "\"string\"", "\"string\"", params, "random_id",
        10000);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);

    //enable write compute param
    props.put(ENABLE_WRITE_COMPUTE, true);
    props.put(INCREMENTAL_PUSH, true);

    TestPushUtils.runPushJob("Test push job", props);
  }
}