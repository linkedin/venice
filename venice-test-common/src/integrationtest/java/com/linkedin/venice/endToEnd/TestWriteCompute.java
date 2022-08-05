package com.linkedin.venice.endToEnd;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskBackdoor;
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
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestWriteCompute {

  private static final int STREAMING_RECORD_SIZE = 1024;

  private VeniceClusterWrapper veniceClusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    int maxMessageSizeInServer = STREAMING_RECORD_SIZE / 2;
    extraProperties.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, Integer.toString(maxMessageSizeInServer));
    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    veniceClusterWrapper = ServiceFactory.getVeniceCluster(
        1,
        2,
        1,
        2,
        1000000,
        false,
        false,
        extraProperties);
  }

  @AfterClass
  public void cleanUp() {
    veniceClusterWrapper.close();
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND, dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testWriteComputeWithHybridLeaderFollowerLargeRecord(boolean writeComputeFromCache, CompressionStrategy compressionStrategy) throws Exception {
    SystemProducer veniceProducer = null;

    try {
      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("write-compute-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      // Records 1-100, id string to name record
      Schema recordSchema = writeSimpleAvroFileWithStringToRecordSchema(inputDir, true);
      Properties h2vProperties = defaultH2VProps(veniceClusterWrapper, inputDirPath, storeName);

      try (
          ControllerClient controllerClient = createStoreForJob(veniceClusterWrapper.getClusterName(), recordSchema, h2vProperties);
          AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL())
          )
      ) {

        ControllerResponse response = controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
            .setHybridRewindSeconds(streamingRewindSeconds)
            .setHybridOffsetLagThreshold(streamingMessageLag)
            .setLeaderFollowerModel(true)
            .setChunkingEnabled(true)
            .setCompressionStrategy(compressionStrategy)
            .setWriteComputationEnabled(true));

        Assert.assertFalse(response.isError());

        // Add a new value schema v2 to store
        SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NESTED_SCHEMA_STRING_V2);
        Assert.assertFalse(schemaResponse.isError());

        // Add WC (Write Compute) schema associated to v2.
        // Note that Write Compute schema needs to be registered manually here because the integration test harness
        // does not create any parent controller. In production, when a value schema is added to a WC-enabled store via
        // a parent controller, it will automatically generate and register its WC schema.
        Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(
            AvroCompatibilityHelper.parse(NESTED_SCHEMA_STRING_V2)
        );
        schemaResponse = controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
        Assert.assertFalse(schemaResponse.isError());

        // H2V push
        runH2V(h2vProperties, 1, controllerClient);

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

//        VersionCreationResponse versionCreationResponse = controllerClient.emptyPush(storeName, Utils.getUniqueString("emptyPushId"), 10000);
//        Assert.assertFalse(versionCreationResponse.isError());
//        final int expectedVersionNumber = 1;
//        TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
//            () -> controllerClient.getStore(storeName).getStore().getCurrentVersion() == expectedVersionNumber);

        //disable the purging of transientRecord buffer using reflection.
        if (writeComputeFromCache) {
          String topicName = Version.composeKafkaTopic(storeName, 1);
          for (VeniceServerWrapper veniceServerWrapper : veniceClusterWrapper.getVeniceServers()) {
            StoreIngestionTaskBackdoor.setPurgeTransientRecordBuffer(
                veniceServerWrapper,
                topicName,
                false);
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
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
   * This test simulates a situation where the stored value schema mismatches with the value schema used by a partial update
   * request. In other words, the partial update request tries to update a field that does not exist in the stored value
   * record due to schema mismatch.
   *
   * In this case, we expect a superset schema that contains fields from all value schema to be used to store the partially
   * updated value record. The partially updated value record should contain original fields as well as the partially updated
   * field.
   */
  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testLatestValueSchemaIsUsed() throws IOException {
    final String storeName = Utils.getUniqueString("partial-update-test-store");
    final String clusterName = veniceClusterWrapper.getClusterName();
    String keySchemaStr = "{\"type\" : \"string\"}";

    // Value schema V3 is a superset schema of V1 and V2.
    String valueSchemaV1Str = loadFileAsString("writecompute/test/PersonV1.avsc");
    String valueSchemaV2Str = loadFileAsString("writecompute/test/PersonV2.avsc");
    String valueSchemaV3Str = loadFileAsString("writecompute/test/PersonV3.avsc");

    Properties properties = new Properties();
    properties.put(VENICE_URL_PROP, veniceClusterWrapper.getRandmonVeniceController().getControllerUrl());
    properties.put(VENICE_STORE_NAME_PROP, storeName);

    UpdateStoreQueryParams storeParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setCompressionStrategy(CompressionStrategy.NO_OP)
        .setBatchGetLimit(2000)
        .setReadQuotaInCU(DEFAULT_PER_ROUTER_READ_QUOTA)
        .setChunkingEnabled(false)
        .setIncrementalPushEnabled(false)
        .setLeaderFollowerModel(true)
        .setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L)
        .setWriteComputationEnabled(true);

    SystemProducer veniceProducer = null;
    try (
        ControllerClient controllerClient = createStoreForJob(clusterName, keySchemaStr, valueSchemaV1Str, properties, storeParams, true);
        AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL())
        )
    ) {
      addValueAndWriteComputeSchemas(storeName, controllerClient, valueSchemaV2Str);
      addValueAndWriteComputeSchemas(storeName, controllerClient, valueSchemaV3Str);
      doEmptyPush(storeName, controllerClient);

      Schema valueSchemaV1 = AvroCompatibilityHelper.parse(valueSchemaV1Str);
      Schema valueSchemaV2 = AvroCompatibilityHelper.parse(valueSchemaV2Str);

      // Step 1: Put a k-v pair where the value uses schema V1
      veniceProducer = getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
      String key = "key1";
      GenericRecord value = new GenericData.Record(valueSchemaV1);
      value.put("name", "Lebron");
      value.put("age", 37);
      sendStreamingRecord(veniceProducer, storeName, key, value);

      // Verify the Put has been persisted
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get("name").toString(), "Lebron");
          assertEquals(retrievedValue.get("age").toString(), "37");
          assertEquals(retrievedValue.get("hometown").toString(), "default_hometown");

        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

      // Step 2: Partially update a field that exists in V2 schema (and it does not exist in V1 schema).
      Schema writeComputeSchemaV2 = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchemaV2);
      UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchemaV2);
      updateBuilder.setNewFieldValue("name", "Lebron James");
      updateBuilder.setNewFieldValue("hometown", "Akron");
      GenericRecord partialUpdateRecord = updateBuilder.build();
      sendStreamingRecord(veniceProducer, storeName, key, partialUpdateRecord);

      // Verify the value record has been partially updated and it uses V3 superset value schema now.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue);
          assertEquals(retrievedValue.get("name").toString(), "Lebron James");
          assertEquals(retrievedValue.get("age").toString(), "37");
          assertEquals(retrievedValue.get("hometown").toString(), "Akron");

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

  private void doEmptyPush(String storeName, ControllerClient controllerClient) {
    VersionCreationResponse versionCreationResponse = controllerClient.emptyPush(storeName, Utils.getUniqueString("emptyPushId"), 10000);
    Assert.assertFalse(versionCreationResponse.isError());
    final int expectedVersionNumber = 1;
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS,
        () -> controllerClient.getStore(storeName).getStore().getCurrentVersion() == expectedVersionNumber);
  }

  private void addValueAndWriteComputeSchemas(String storeName, ControllerClient controllerClient, String valueSchemaStr) {
    SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, valueSchemaStr);
    Assert.assertFalse(schemaResponse.isError(), "Got error: " + schemaResponse.getError());

    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(
        AvroCompatibilityHelper.parse(valueSchemaStr)
    );
    schemaResponse = controllerClient.addDerivedSchema(storeName, schemaResponse.getId(), writeComputeSchema.toString());
    Assert.assertFalse(schemaResponse.isError(), "Got error: " + schemaResponse.getError());
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

  /**
   * Blocking, waits for new version to go online
   */
  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    String jobName = Utils.getUniqueString("write-compute-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore().getCurrentVersion() == expectedVersionNumber);
    }
  }
}
