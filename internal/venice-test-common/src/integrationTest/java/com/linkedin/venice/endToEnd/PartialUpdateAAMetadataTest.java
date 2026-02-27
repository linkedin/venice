package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.verifyConsumerThreadPoolFor;
import static com.linkedin.venice.utils.IntegrationTestReadUtils.readValue;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.ConsumerPoolType;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


/**
 * This class includes tests on partial update A/A metadata and lifecycle scenarios:
 * schema evolution with RMD, consumer pool transitions, and RMD type conversion.
 */
public class PartialUpdateAAMetadataTest extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;
  private static final int ASSERTION_TIMEOUT_MS = 30_000;

  private static final int REPLICATION_FACTOR = 2;

  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testUpdateValueWithOldSchemaWithFieldLevelRMD() {
    final String storeName = Utils.getUniqueString("convertToFieldLevel");
    String parentControllerUrl = getParentControllerUrl();
    Schema schemaV1 = AvroCompatibilityHelper.parse(loadFileAsString("UserV1.avsc"));
    Schema schemaV2 = AvroCompatibilityHelper.parse(loadFileAsString("UserV2.avsc"));

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), schemaV1.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      assertCommand(parentControllerClient.addValueSchema(storeName, schemaV2.toString()));
      UpdateStoreQueryParams updateStoreParams2 = new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams2));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      response = parentControllerClient.emptyPush(storeName, "test_push_id_v2", 1000);
      assertEquals(response.getVersion(), 2);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
    VeniceClusterWrapper veniceClusterWrapper = getClusterDC0();
    String key = "highway";
    try (VeniceSystemProducer veniceProducer =
        getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM)) {
      // Send first value with old schema;
      GenericRecord record = new GenericData.Record(schemaV1);
      record.put("id", "101");
      record.put("name", "U.S. 101");
      sendStreamingRecord(veniceProducer, storeName, key, record);
      // Send second value with new schema; Without default schema carry fix, it will fail SIT ingestion.
      GenericRecord recordV2 = new GenericData.Record(schemaV2);
      recordV2.put("id", "280");
      recordV2.put("name", "Interstate 280");
      sendStreamingRecord(veniceProducer, storeName, key, recordV2);
    }

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord value = readValue(storeReader, key);
          assertNotNull(value, "Key " + key + " should not be missing!");
          assertEquals(value.get("id").toString(), "280");
          assertEquals(value.get("name").toString(), "Interstate 280");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testEnablePartialUpdateOnActiveActiveStore() {
    final String storeName = Utils.getUniqueString("testEnablePartialUpdateOnActiveActiveStore");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    PubSubTopic storeVersionTopicV1 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 1));
    PubSubTopic storeVersionTopicV2 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopicPartition realTimeTopicPartition;
    PubSubTopicPartition storeVersionTopicPartitionV1 = new PubSubTopicPartitionImpl(storeVersionTopicV1, 0);
    PubSubTopicPartition storeVersionTopicPartitionV2 = new PubSubTopicPartitionImpl(storeVersionTopicV2, 0);
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      TestUtils.assertCommand(parentControllerClient.getStore(storeName)).getStore();
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(1L)
              .setHybridOffsetLagThreshold(1L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      // Generate v1 without AA and Write-compute:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV1.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      StoreInfo storeInfo = TestUtils.assertCommand(parentControllerClient.getStore(storeName)).getStore();
      String realTimeTopicName = Utils.getRealTimeTopicName(storeInfo);
      PubSubTopic realTimeTopic = PUB_SUB_TOPIC_REPOSITORY.getTopic(realTimeTopicName);
      realTimeTopicPartition = new PubSubTopicPartitionImpl(realTimeTopic, 0);

      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            realTimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            1);
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            storeVersionTopicPartitionV1,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);
      });

      // Enable write-compute for v1:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER => CURRENT_VERSION_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsEnableWriteCompute =
          new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      updateStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsEnableWriteCompute));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            realTimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
            1,
            1);
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            storeVersionTopicPartitionV1,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);
      });
      // Disable write-compute for v1:
      // leader: CURRENT_VERSION_AAWC_LEADER => CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsDisableWriteCompute =
          new UpdateStoreQueryParams().setWriteComputationEnabled(false);
      updateStoreResponse = parentControllerClient
          .retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsDisableWriteCompute));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            realTimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            1);
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            storeVersionTopicPartitionV1,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);
      });
      // Enable AA and push a new version to get v2, to verify pool change
      // For v1 without AA:
      // leader: CURRENT_VERSION_NON_AAWC_LEADER => NON_CURRENT_VERSION_NON_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER => CURRENT_VERSION_AAWC_LEADER
      // For v2 with AA:
      // leader : CURRENT_VERSION_AAWC_LEADER
      // follower: CURRENT_VERSION_NON_AAWC_LEADER
      UpdateStoreQueryParams updateStoreParamsForEnablingAA =
          new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
      updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParamsForEnablingAA));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
      response = parentControllerClient.emptyPush(storeName, "test_push_id for 2", 1000);

      assertEquals(response.getVersion(), 2);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV2.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
      TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
        // Version 1 has active-active and write compute disabled, so each server host will ingest from local data
        // center for leader.
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            realTimeTopicPartition,
            ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            1);
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV1,
            storeVersionTopicPartitionV1,
            ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);
        // Version 2 has active-active enabled, so each server host will ingest from two data centers for leader.
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV2,
            realTimeTopicPartition,
            ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
            childDatacenters.size(),
            1);
        verifyConsumerThreadPoolFor(
            multiRegionMultiClusterWrapper,
            CLUSTER_NAME,
            storeVersionTopicV2,
            storeVersionTopicPartitionV2,
            ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
            1,
            REPLICATION_FACTOR - 1);
      });
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();

    String key1 = "key1";

    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("name", "value-level TS");
    record.put("floatArray", Collections.emptyList());
    record.put("stringMap", Collections.emptyMap());

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, record, 10000L);
    }

    for (int i = 0; i < childDatacenters.size(); i++) {
      VeniceClusterWrapper cluster = getCluster(i);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("value-level TS"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
    }

    // Enable AA and push a new version to get v3, to verify pool change
    // For v2 with AA:
    // leader : CURRENT_VERSION_AAWC_LEADER => NON_CURRENT_VERSION_AAWC_LEADER
    // follower: CURRENT_VERSION_NON_AAWC_LEADER => NON_CURRENT_VERSION_NON_AAWC_LEADER
    // For v3 with AA:
    // leader : CURRENT_VERSION_AAWC_LEADER
    // follower: CURRENT_VERSION_NON_AAWC_LEADER
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, "dummyInputPath", storeName);
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, veniceCluster.getPubSubBrokerWrapper().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    IntegrationTestPushUtils.runVPJ(props);
    PubSubTopic storeVersionTopicV3 = PUB_SUB_TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, 3));
    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      TestUtils.waitForNonDeterministicPushCompletion(
          storeVersionTopicV3.getName(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
    PubSubTopicPartition storeVersionTopicPartitionV3 = new PubSubTopicPartitionImpl(storeVersionTopicV3, 0);
    TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
      // Version 2 become backup version.
      verifyConsumerThreadPoolFor(
          multiRegionMultiClusterWrapper,
          CLUSTER_NAME,
          storeVersionTopicV2,
          realTimeTopicPartition,
          ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL,
          childDatacenters.size(),
          1);
      verifyConsumerThreadPoolFor(
          multiRegionMultiClusterWrapper,
          CLUSTER_NAME,
          storeVersionTopicV2,
          storeVersionTopicPartitionV2,
          ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1,
          REPLICATION_FACTOR - 1);
      verifyConsumerThreadPoolFor(
          multiRegionMultiClusterWrapper,
          CLUSTER_NAME,
          storeVersionTopicV3,
          realTimeTopicPartition,
          ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL,
          childDatacenters.size(),
          1);
      verifyConsumerThreadPoolFor(
          multiRegionMultiClusterWrapper,
          CLUSTER_NAME,
          storeVersionTopicV3,
          storeVersionTopicPartitionV3,
          ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL,
          1,
          REPLICATION_FACTOR - 1);
    });
    // Need to create a new producer to update partial update config from store response.
    UpdateBuilder builder = new UpdateBuilderImpl(partialUpdateSchema);
    builder.setNewFieldValue("name", "field-level TS");
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, builder.build(), 10001L);
    }

    for (int i = 0; i < childDatacenters.size(); i++) {
      VeniceClusterWrapper cluster = getCluster(i);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("field-level TS"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testConvertRmdType() {
    final String storeName = Utils.getUniqueString("testConvertRmdType");
    String parentControllerUrl = getParentControllerUrl();
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV1.avsc"));
    Schema partialUpdateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setPartitionCount(1)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setChunkingEnabled(true)
              .setRmdChunkingEnabled(true)
              .setHybridRewindSeconds(1L)
              .setHybridOffsetLagThreshold(1L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper veniceCluster = getClusterDC0();
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";

    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("name", "value-level TS");
    record.put("floatArray", Collections.emptyList());
    record.put("stringMap", Collections.emptyMap());

    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, record, 10000L);
      sendStreamingRecord(veniceProducer, storeName, key2, record, 10000L);
      sendStreamingRecord(veniceProducer, storeName, key3, record, 10000L);
    }

    for (int i = 0; i < childDatacenters.size(); i++) {
      VeniceClusterWrapper cluster = getCluster(i);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("value-level TS"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setWriteComputationEnabled(true);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());
    }

    VeniceClusterWrapper clusterWrapper = getClusterDC0();
    for (int port: clusterWrapper.getVeniceServers()
        .stream()
        .map(VeniceServerWrapper::getPort)
        .collect(Collectors.toList())) {
      clusterWrapper.stopAndRestartVeniceServer(port);
    }

    // New PUT record for KEY1
    record = new GenericData.Record(valueSchema);
    record.put("name", "field-level TS PUT");
    record.put("floatArray", Collections.emptyList());
    record.put("stringMap", Collections.emptyMap());
    // New UPDATE record for KEY3
    UpdateBuilder builder = new UpdateBuilderImpl(partialUpdateSchema);
    builder.setNewFieldValue("name", "field-level TS UPDATE");
    try (VeniceSystemProducer veniceProducer = getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM)) {
      sendStreamingRecord(veniceProducer, storeName, key1, record, 10001L);
      sendStreamingDeleteRecord(veniceProducer, storeName, key2, 10001L);
      sendStreamingRecord(veniceProducer, storeName, key3, builder.build(), 10001L);
    }
    for (int i = 0; i < childDatacenters.size(); i++) {
      VeniceClusterWrapper cluster = getCluster(i);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(ASSERTION_TIMEOUT_MS, TimeUnit.MILLISECONDS, true, () -> {
          try {
            GenericRecord valueRecord = readValue(storeReader, key2);
            assertNull(valueRecord);

            valueRecord = readValue(storeReader, key1);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("field-level TS PUT"));

            valueRecord = readValue(storeReader, key3);
            assertNotNull(valueRecord);
            assertEquals(valueRecord.get("name"), new Utf8("field-level TS UPDATE"));
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }
}
