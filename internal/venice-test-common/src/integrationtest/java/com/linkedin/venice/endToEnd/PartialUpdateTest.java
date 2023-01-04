package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestPushUtils.NESTED_SCHEMA_STRING;
import static com.linkedin.venice.utils.TestPushUtils.NESTED_SCHEMA_STRING_V2;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestPushUtils.loadFileAsString;
import static com.linkedin.venice.utils.TestPushUtils.writeSimpleAvroFileWithStringToRecordSchema;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskBackdoor;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update (Write Compute) with a setup that has both the parent and child controllers.
 */
public class PartialUpdateTest {
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 120_000;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    this.multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(controllerProps)),
        Optional.of(new Properties(controllerProps)),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    this.childDatacenters = multiColoMultiClusterWrapper.getClusters();
    List<VeniceControllerWrapper> parentControllers = multiColoMultiClusterWrapper.getParentControllers();
    if (parentControllers.size() != 1) {
      throw new IllegalStateException("Expect only one parent controller. Got: " + parentControllers.size());
    }
    this.parentController = parentControllers.get(0);
  }

  /**
   * This test simulates a situation where the stored value schema mismatches with the value schema used by a partial update
   * request. In other words, the partial update request tries to update a field that does not exist in the stored value
   * record due to schema mismatch.
   *
   * In this case, we expect a superset schema that contains fields from all value schema to be used to store the partially
   * updated value record. The partially updated value record should contain original fields as well as the partially updated
   * field.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testUpdateWithSupersetSchema() throws IOException {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = parentController.getControllerUrl();
    String keySchemaStr = "{\"type\" : \"string\"}";
    Schema valueSchemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));
    Schema valueSchemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV2.avsc"));
    String valueFieldName = "name";

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaV1.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setLeaderFollowerModel(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      Assert.assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      assertCommand(parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString()));
    }

    SystemProducer veniceProducer = null;
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      // Step 1. Put a value record.
      veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM);
      String key = "key1";
      GenericRecord value = new GenericData.Record(valueSchemaV1);
      value.put(valueFieldName, "Lebron");
      value.put("age", 37);
      sendStreamingRecord(veniceProducer, storeName, key, value);

      // Verify the Put has been persisted
      TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron");
          assertEquals(retrievedValue.get("age").toString(), "37");

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 2: Partially update a field that exists in V2 schema (and it does not exist in V1 schema).
      Schema writeComputeSchemaV2 =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV2);
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchemaV2);
      updateBuilder.setNewFieldValue(valueFieldName, "Lebron James");
      updateBuilder.setNewFieldValue("hometown", "Akron");
      GenericRecord partialUpdateRecord = updateBuilder.build();
      sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);

      // Verify the value record has been partially updated and it uses V3 superset value schema now.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get(valueFieldName).toString(), "Lebron James"); // Updated field
          assertEquals(retrievedValue.get("age").toString(), "37");
          assertEquals(retrievedValue.get("hometown").toString(), "Akron"); // Updated field

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  @Test(timeOut = 120
      * Time.MS_PER_SECOND, dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testWriteComputeWithHybridLeaderFollowerLargeRecord(
      boolean writeComputeFromCache,
      CompressionStrategy compressionStrategy) throws Exception {

    SystemProducer veniceProducer = null;

    try {
      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("write-compute-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      String parentControllerURL = parentController.getControllerUrl();
      // Records 1-100, id string to name record
      Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir, true);
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      Properties vpjProperties = TestPushUtils.defaultVPJProps(parentControllerURL, inputDirPath, storeName);
      try (ControllerClient controllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURL);
          AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName)
                  .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {

        String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
        String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
        assertCommand(controllerClient.createNewStore(storeName, "test_owner", keySchemaStr, valueSchemaStr));

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setLeaderFollowerModel(true)
                .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
                .setChunkingEnabled(true)
                .setCompressionStrategy(compressionStrategy)
                .setWriteComputationEnabled(true)
                .setHybridRewindSeconds(10L)
                .setHybridOffsetLagThreshold(2L));

        Assert.assertFalse(response.isError());

        // Add a new value schema v2 to store
        SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NESTED_SCHEMA_STRING_V2);
        Assert.assertFalse(schemaResponse.isError());

        // Add WC (Write Compute) schema associated to v2.
        // Note that Write Compute schema needs to be registered manually here because the integration test harness
        // does not create any parent controller. In production, when a value schema is added to a WC-enabled store via
        // a parent controller, it will automatically generate and register its WC schema.
        Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance()
            .convertFromValueRecordSchema(AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING_V2));
        schemaResponse =
            controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
        Assert.assertFalse(schemaResponse.isError());

        // VPJ push
        String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
        try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
          runVPJ(vpjProperties, 1, childControllerClient);
        }

        // Verify records (note, records 1-100 have been pushed)
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
              assertEquals(value.get("age"), -1);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // disable the purging of transientRecord buffer using reflection.
        if (writeComputeFromCache) {
          String topicName = Version.composeKafkaTopic(storeName, 1);
          for (VeniceServerWrapper veniceServerWrapper: veniceClusterWrapper.getVeniceServers()) {
            StoreIngestionTaskBackdoor.setPurgeTransientRecordBuffer(veniceServerWrapper, topicName, false);
          }
        }

        // Do not send large record to RT; RT doesn't support chunking
        veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
        String key = String.valueOf(101);
        Schema valueSchema = AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING);
        GenericRecord value = new GenericData.Record(valueSchema);
        char[] chars = new char[100];
        Arrays.fill(chars, 'f');
        String firstName = new String(chars);
        Arrays.fill(chars, 'l');
        String lastName = new String(chars);
        value.put("firstName", firstName);
        value.put("lastName", lastName);
        sendStreamingRecord(veniceProducer, storeName, key, value);

        // Verify the streaming record
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), firstName);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record
        Arrays.fill(chars, 'u');
        String updatedFirstName = new String(chars);
        final int updatedAge = 1;
        UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName);
        updateBuilder.setNewFieldValue("age", updatedAge);
        GenericRecord partialUpdateRecord = updateBuilder.build();

        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
            assertEquals(retrievedValue.get("age"), updatedAge);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'v');
        String updatedFirstName1 = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName1);
        GenericRecord partialUpdateRecord1 = updateBuilder.build();
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord1);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName1);
            assertEquals(retrievedValue.get("lastName").toString(), lastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Delete the record
        sendStreamingRecord(veniceProducer, storeName, key, null);
        // Verify the delete
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNull(retrievedValue, "Key " + key + " should be missing!");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'w');
        String updatedFirstName2 = new String(chars);
        Arrays.fill(chars, 'g');
        String updatedLastName = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName2);
        updateBuilder.setNewFieldValue("lastName", updatedLastName);
        updateBuilder.setNewFieldValue("age", 2);
        GenericRecord partialUpdateRecord2 = updateBuilder.build();

        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord2);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName2);
            assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
            assertEquals(retrievedValue.get("age"), 2);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update the record again
        Arrays.fill(chars, 'x');
        String updatedFirstName3 = new String(chars);

        updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
        updateBuilder.setNewFieldValue("firstName", updatedFirstName3);
        GenericRecord partialUpdateRecord3 = updateBuilder.build();
        sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord3);
        // Verify the update
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            assertEquals(retrievedValue.get("firstName").toString(), updatedFirstName3);
            assertEquals(retrievedValue.get("lastName").toString(), updatedLastName);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  /**
   * Blocking, waits for new version to go online
   */
  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    String jobName = Utils.getUniqueString("write-compute-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          10,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiColoMultiClusterWrapper);
  }
}
