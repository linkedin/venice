package com.linkedin.venice.hadoop;

import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.hadoop.VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFER_VERSION_SWAP;
import static com.linkedin.venice.hadoop.VenicePushJob.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.hadoop.VenicePushJob.KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJob.SUPPRESS_END_OF_PUSH_MESSAGE;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.getLatestPathOfInputDirectory;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema2;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleVsonFileWithUserSchema;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.hadoop.partitioner.BuggyOffsettingMapReduceShufflePartitioner;
import com.linkedin.venice.hadoop.partitioner.BuggySprayingMapReduceShufflePartitioner;
import com.linkedin.venice.hadoop.partitioner.NonDeterministicVenicePartitioner;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class contains only end-to-end/integration tests for VenicePushJob
 */

public class TestVenicePushJob {
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
  protected static Schema writeComplicatedAvroFileWithUserSchema(File parentDir, boolean addFieldWithDefaultValue)
      throws IOException {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "      { \"name\": \"id\", \"type\": \"string\"},\n" + "      {\n"
        + "       \"name\": \"value\",\n" + "       \"type\": {\n" + "           \"type\": \"record\",\n"
        + "           \"name\": \"ValueRecord\",\n" + "           \"fields\" : [\n"
        + "              {\"name\": \"favorite_number\", \"type\": \"int\"}\n";
    if (addFieldWithDefaultValue) {
      schemaStr += ",{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    }
    schemaStr += "           ]\n" + "        }\n" + "      }\n" + " ]\n" + "}";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, file);

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
      return schema;
    }
  }

  /**
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  protected static Schema writeSimpleAvroFileWithDifferentUserSchema(File parentDir) throws IOException {
    String schemaStr = "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
        + "  \"name\": \"User\",     " + "  \"fields\": [           " + "       { \"name\": \"" + DEFAULT_KEY_FIELD_PROP
        + "\", \"type\": \"string\" },  " + "       { \"name\": \"" + DEFAULT_VALUE_FIELD_PROP
        + "\", \"type\": \"string\" },  " + "       { \"name\": \"age\", \"type\": \"int\" },  "
        + "       { \"name\": \"company\", \"type\": \"string\" }  " + "  ] " + " } ";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user_with_different_schema.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, file);

      String name = "test_name_";
      String company = "company_";
      for (int i = 1; i <= 100; ++i) {
        GenericRecord user = new GenericData.Record(schema);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, name + i);
        user.put("age", i);
        user.put("company", company + i);
        dataFileWriter.append(user);
      }
      return schema;
    }
  }

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().sslToStorageNodes(true).build();
    veniceCluster = ServiceFactory.getVeniceCluster(options); // Now with SSL!
    controllerClient = new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getRandomRouterURL());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Inconsistent file.* schema found.*")
  public void testRunJobWithInputHavingDifferentSchema() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    writeSimpleAvroFileWithDifferentUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    TestWriteUtils.runPushJob("Test push job", props);
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
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing key field
    props.put(KEY_FIELD_PROP, "id1");

    TestWriteUtils.runPushJob("Test push job", props);

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   *This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*Could not find field: name1.*")
  public void testRunJobWithInvalidValueField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing value field
    props.put(VALUE_FIELD_PROP, "name1");

    TestWriteUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   *This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*Could not find field: name1.*")
  public void testRunJobWithInvalidValueFieldVson() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleVsonFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    // Override with not-existing value field
    props.put(KEY_FIELD_PROP, "");
    props.put(VALUE_FIELD_PROP, "name1");

    TestWriteUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  /**
   * This is a fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*should not have sub directory.*")
  public void testRunJobWithSubDirInInputDir() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Create sub directory
    File subDir = new File(inputDir, "sub-dir");
    subDir.mkdir();

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    TestWriteUtils.runPushJob("Test push job", props);

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
    File inputDir_v1 = new File(inputDir, "v1");
    inputDir_v1.mkdir();
    File inputDir_v2 = new File(inputDir, "v2");
    inputDir_v2.mkdir();
    File inputDir_v2_v1 = new File(inputDir_v2, "v1");
    inputDir_v2_v1.mkdir();
    File inputDir_v2_v2 = new File(inputDir_v2, "v2");
    inputDir_v2_v2.mkdir();
    File inputDir_v2_file = new File(inputDir_v2, "v3.avro"); // Added to ensure lexically greater files do not get
                                                              // resolved
    inputDir_v2_file.createNewFile();

    FileSystem fs = FileSystem.get(new Configuration());

    Assert.assertEquals(
        getLatestPathOfInputDirectory("file:" + inputDir.getAbsolutePath() + "/#LATEST", fs).toString(),
        "file:" + inputDir_v2.getAbsolutePath(),
        "VenicePushJob should parse #LATEST to latest directory when it is in the last level in the input path");

    Assert.assertEquals(
        getLatestPathOfInputDirectory("file:" + inputDir.getAbsolutePath() + "/#LATEST/v1", fs).toString(),
        "file:" + inputDir_v2_v1.getAbsolutePath(),
        "VenicePushJob should parse #LATEST to latest directory when it is only in an intermediate level in the input path");

    Assert.assertEquals(
        getLatestPathOfInputDirectory("file:" + inputDir.getAbsolutePath() + "/#LATEST/#LATEST", fs).toString(),
        "file:" + inputDir_v2_v2.getAbsolutePath(),
        "VenicePushJob should parse all occurrences of #LATEST to respective latest directories");

    Assert.assertEquals(
        getLatestPathOfInputDirectory("file:" + inputDir.getAbsolutePath() + "/#LATEST/#LATEST/", fs).toString(),
        "file:" + inputDir_v2_v2.getAbsolutePath(),
        "VenicePushJob should parse #LATEST to latest directory to respective latest directories");
  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Key schema mis-match for store.*")
  public void testRunJobWithDifferentKeySchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = Utils.getUniqueString("store");
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster.getClusterName(), recordSchema, props).close();
    String jobName = "Test push job";

    // Run job with different key schema (from 'string' to 'int')
    props.setProperty(KEY_FIELD_PROP, "age");
    TestWriteUtils.runPushJob("Test push job", props);

  }

  /**
   * This is a (mostly) fast test as long as @BeforeMethod doesn't create a cluster
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Failed to validate value schema.*")
  public void testRunJobMultipleTimesWithInCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster.getClusterName(), recordSchema, props).close();
    String jobName = "Test push job";
    // Run job with different value schema (from 'string' to 'int')
    props.setProperty(VALUE_FIELD_PROP, "age");
    props.setProperty(CONTROLLER_REQUEST_RETRY_ATTEMPTS, "2");
    TestWriteUtils.runPushJob("Test push job", props);

  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobWithEOPSuppressed() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = Utils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir, false);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(SUPPRESS_END_OF_PUSH_MESSAGE, "true");
    createStoreForJob(veniceCluster.getClusterName(), recordSchema, props);
    TestWriteUtils.runPushJob("Test push job", props);

    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      MultiStoreStatusResponse response = controllerClient.getFutureVersions(veniceCluster.getClusterName(), storeName);
      Assert.assertEquals(response.getStoreStatusMap().size(), 1);
      controllerClient.writeEndOfPush(storeName, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
    });

  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobWithDeferredVersionSwap() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = Utils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);
    Schema recordSchema = writeSimpleAvroFileWithStringToStringSchema(inputDir, false);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(DEFER_VERSION_SWAP, "true");
    createStoreForJob(veniceCluster.getClusterName(), recordSchema, props);

    // Create Version 1
    TestWriteUtils.runPushJob("Test push job", props);

    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      MultiStoreStatusResponse response = controllerClient.getFutureVersions(veniceCluster.getClusterName(), storeName);
      Assert.assertEquals(response.getStoreStatusMap().size(), 1);
      controllerClient.overrideSetActiveVersion(storeName, 1);
    });

    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
    });

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
    String storeName = Utils.getUniqueString("store");
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
    String storeName = Utils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();

    // disable WriteCompute in store
    params.setWriteComputationEnabled(false);
    params.setIncrementalPushEnabled(true);

    controllerClient.createNewStoreWithParameters(storeName, "owner", "\"string\"", "\"string\"", params);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    // enable write compute param
    props.put(ENABLE_WRITE_COMPUTE, true);
    props.put(INCREMENTAL_PUSH, true);

    TestWriteUtils.runPushJob("Test push job", props);
  }

  /**
   * Testing write compute enabled job where the store does not have LeaderFollower enabled
   * @throws Exception
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Write compute is only available for incremental push jobs.*")
  public void testWCBatchJob() throws Exception {
    File inputDir = getTempDataDirectory();
    String storeName = Utils.getUniqueString("store");
    String routerUrl = veniceCluster.getRandomRouterURL();
    ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setWriteComputationEnabled(true);
    params.setIncrementalPushEnabled(false);

    controllerClient.createNewStoreWithParameters(storeName, "owner", "\"string\"", "\"string\"", params);

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    // enable write compute param
    props.put(ENABLE_WRITE_COMPUTE, true);
    props.put(INCREMENTAL_PUSH, false);

    TestWriteUtils.runPushJob("Test push job", props);
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Exception or error caught during VenicePushJob.*")
  public void testRunJobWithBuggySprayingMapReduceShufflePartitioner() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setPartitionCount(3)));
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    TestWriteUtils.runPushJob(
        "Test push job",
        props,
        job -> job.setMapRedPartitionerClass(BuggySprayingMapReduceShufflePartitioner.class));
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Exception or error caught during VenicePushJob.*")
  public void testRunJobWithBuggyOffsettingMapReduceShufflePartitioner() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA).setPartitionCount(3)));
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    TestWriteUtils.runPushJob(
        "Test push job",
        props,
        job -> job.setMapRedPartitionerClass(BuggyOffsettingMapReduceShufflePartitioner.class));
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Exception or error caught during VenicePushJob.*")
  public void testRunJobWithNonDeterministicPartitioner() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    String nonDeterministicPartitionerClassName = NonDeterministicVenicePartitioner.class.getName();
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setPartitionCount(3)
                .setPartitionerClass(nonDeterministicPartitionerClassName)));
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(VENICE_PARTITIONERS, nonDeterministicPartitionerClassName);

    TestWriteUtils.runPushJob("Test push job", props);
    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT, description = "KIF repush should copy all data including recent incPush2RT to new VT")
  public void testKIFRepushForIncrementalPushStores() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setPartitionCount(2)
                .setIncrementalPushEnabled(true)));
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);

    // create a batch version.
    TestWriteUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));

    writeSimpleAvroFileWithUserSchema2(inputDir);
    props.setProperty(INCREMENTAL_PUSH, "true");
    TestWriteUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1));
    props.setProperty(INCREMENTAL_PUSH, "false");

    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      // data from full push
      for (int i = 1; i <= 50; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
      // data updated and added by inc-push
      for (int i = 51; i <= 150; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
      }
    }

    // setup repush job settings
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getKafka().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    // This repush should succeed
    TestWriteUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));

    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      // data from full push
      for (int i = 1; i <= 50; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
      // data updated and added by inc-push
      for (int i = 51; i <= 150; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKIFRepushFetch(boolean chunkingEnabled) throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    veniceCluster.getNewStore(storeName);
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setPartitionCount(2)
                .setIncrementalPushEnabled(true)
                .setWriteComputationEnabled(true)));
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, "true");
    // create a batch version.
    TestWriteUtils.runPushJob("Test push job", props);
    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 1; i <= 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }

    // setup repush job settings, without any broker url or topic name
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_FABRIC, "dc-0");
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    // convert to RT policy
    TestUtils.assertCommand(
        veniceCluster.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1)
                .setHybridRewindSeconds(0)
                .setChunkingEnabled(chunkingEnabled)));
    // Run the repush job, it should still pass
    TestWriteUtils.runPushJob("Test push job", props);

    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      for (int i = 1; i <= 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }
  }
}
