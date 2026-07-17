package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithoutFlush;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestProducerBatching extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;

  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testUpdateWithBatchingEnabled() {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = getParentControllerUrl();
    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV2.avsc"));
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setWriteComputationEnabled(true)
              .setPartitionCount(1)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      veniceClusterWrapper.waitVersion(storeName, 1);
    }
    SystemProducer veniceProducer;
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    // Put some big enough delay number to make sure they all get sent in a batch for final validation.
    Pair<String, String> additionalConfig = new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_INTERVAL_MS, "1000");
    Pair<String, String> additionalConfig2 = new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_BUFFER_SIZE_IN_BYTES, "1024000");
    veniceProducer = IntegrationTestPushUtils
        .getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM, additionalConfig, additionalConfig2);

    String key = "key";
    // Message 1: Will be compacted by Message 2
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 99);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value);

    // Message 2: Should be produced.
    value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 100);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value);

    // Message 3: Should be produced (logical TS)
    value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 101);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value, 100L);

    String key2 = "key2";
    // Message 4: Will be compacted by Message 7
    value = new GenericData.Record(valueSchema);
    value.put("name", "DEN");
    value.put("age", 2023);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    // Message 5: Will be compacted by Message 7
    value = new GenericData.Record(valueSchema);
    value.put("name", "CLE");
    value.put("age", 2024);
    value.put("intArray", Arrays.asList(1, 2, 3));
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    // Message 6: Will be compacted by Message 7
    value = new UpdateBuilderImpl(updateSchema).setNewFieldValue("age", 2025)
        .setNewFieldValue("intArray", Arrays.asList(4, 5, 6))
        .setEntriesToAddToMapField("intMap", Collections.singletonMap("a", 1))
        .build();
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    // Message 7: Should be produced.
    value = new UpdateBuilderImpl(updateSchema).setNewFieldValue("name", "OKC")
        .setElementsToAddToListField("intArray", Arrays.asList(7, 8))
        .setElementsToRemoveFromListField("intArray", Arrays.asList(3, 4))
        .setNewFieldValue("intMap", Collections.singletonMap("b", 2))
        .build();
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
          assertEquals(retrievedValue.get("name").toString(), "val");
          assertEquals(retrievedValue.get("age"), 100);

          retrievedValue = readValue(storeReader, key2);
          assertNotNull(retrievedValue, "Key " + key2 + " should not be missing!");
          assertEquals(retrievedValue.get("name").toString(), "OKC");
          assertNotNull(retrievedValue.get("intArray"));
          assertNotNull(retrievedValue.get("intMap"));
          List<Integer> intArray = (List<Integer>) retrievedValue.get("intArray");
          Map<Utf8, Integer> intMap = (Map<Utf8, Integer>) retrievedValue.get("intMap");
          assertEquals(intArray.size(), 4);
          assertTrue(intArray.contains(5));
          assertTrue(intArray.contains(6));
          assertTrue(intArray.contains(7));
          assertTrue(intArray.contains(8));
          assertEquals(intMap.size(), 1);
          assertEquals((int) intMap.get(new Utf8("b")), 2);
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    } finally {
      veniceProducer.stop();
    }

    // Consume all the RT messages and validated how many data records were produced.
    PubSubBrokerWrapper pubSubBrokerWrapper =
        childDatacenters.get(0).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {
      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName, 1)), 0),
          PubSubSymbolicPosition.EARLIEST,
          false);
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(1000 * Time.MS_PER_SECOND);
      int messageCount = 0;
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        List<DefaultPubSubMessage> pubSubMessages = entry.getValue();
        for (DefaultPubSubMessage message: pubSubMessages) {
          if (!message.getKey().isControlMessage()) {
            messageCount += 1;
          }
        }
      }
      Assert.assertEquals(messageCount, 3);
    }
  }

  /**
   * Reproduces the oversized-merge scenario: several batched partial updates for the same key touch different large
   * fields, so the fully merged UPDATE payload exceeds {@code DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   * (950KB). Since UPDATE does not support chunking, {@link com.linkedin.venice.writer.BatchingVeniceWriter} must split
   * the merge into multiple under-limit messages instead of throwing {@code RecordTooLargeException}.
   *
   * The scalar {@code age} field is re-set across the split boundary (222 early, 111 late). Because batched records use
   * the default logical timestamp, Active/Active DCR resolves by broker {@code messageTimestamp}; the split messages must
   * therefore be produced at strictly increasing timestamps so the latest write (111) wins. Note that if they shared a
   * timestamp, DCR would break the tie by value comparison and keep 222 ({@code Integer.hashCode(222) > hashCode(111)}),
   * so asserting {@code age == 111} verifies both the split and the recency-preserving timestamp behavior end to end.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testBatchedPartialUpdatesExceedingSizeLimitAreSplit() {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = getParentControllerUrl();
    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("CollectionRecordV2.avsc"));
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setWriteComputationEnabled(true)
              .setPartitionCount(1)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      veniceClusterWrapper.waitVersion(storeName, 1);
    }

    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    // Long interval so all three updates land in a single batch; large buffer so the batch is not flushed early by
    // size.
    Pair<String, String> additionalConfig = new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_INTERVAL_MS, "3000");
    Pair<String, String> additionalConfig2 =
        new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_BUFFER_SIZE_IN_BYTES, "10485760");
    SystemProducer veniceProducer = IntegrationTestPushUtils
        .getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM, additionalConfig, additionalConfig2);

    // Two ~500KB fields set by different updates so the merged payload (~1MB) exceeds the 950KB per-message limit,
    // while every individual update stays under the limit.
    final int nameLength = 512 * 1024;
    char[] nameChars = new char[nameLength];
    Arrays.fill(nameChars, 'x');
    String bigName = new String(nameChars);
    final int intArraySize = 180000;
    List<Integer> bigIntArray = new ArrayList<>(intArraySize);
    for (int i = 0; i < intArraySize; i++) {
      bigIntArray.add(10000 + i);
    }

    String key = "bigKey";
    try {
      // Update 1: early value for the scalar "age" plus a large "name" field.
      GenericRecord update1 =
          new UpdateBuilderImpl(updateSchema).setNewFieldValue("age", 222).setNewFieldValue("name", bigName).build();
      sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, update1);

      // Update 2: a large "intArray" field. Merging updates 1 and 2 already exceeds the limit, forcing a split.
      GenericRecord update2 = new UpdateBuilderImpl(updateSchema).setNewFieldValue("intArray", bigIntArray).build();
      sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, update2);

      // Update 3: re-set the scalar "age" to the latest value. It lands in the later split message.
      GenericRecord update3 = new UpdateBuilderImpl(updateSchema).setNewFieldValue("age", 111).build();
      sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, update3);

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          try {
            GenericRecord retrievedValue = readValue(storeReader, key);
            assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
            // Data from both split messages must be preserved.
            assertEquals(retrievedValue.get("name").toString().length(), nameLength, "Large name field must survive");
            assertNotNull(retrievedValue.get("intArray"), "Large intArray field must survive");
            assertEquals(((List<Integer>) retrievedValue.get("intArray")).size(), intArraySize);
            // The scalar re-set across the split boundary must resolve to the latest write, not the value-comparison
            // tie.
            assertEquals(retrievedValue.get("age"), 111, "Latest write to the straddling scalar must win");
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    } finally {
      veniceProducer.stop();
    }

    // The oversized merge must have been split into at least two data messages on the real-time topic.
    PubSubBrokerWrapper pubSubBrokerWrapper =
        childDatacenters.get(0).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {
      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName, 1)), 0),
          PubSubSymbolicPosition.EARLIEST,
          false);
      // Poll in a bounded loop with a short per-poll timeout and an overall deadline so a regression that drops
      // messages fails fast instead of blocking on a single long poll, and a late-arriving message is not undercounted.
      int messageCount = 0;
      long deadlineMs = System.currentTimeMillis() + 30 * Time.MS_PER_SECOND;
      while (System.currentTimeMillis() < deadlineMs) {
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(2 * Time.MS_PER_SECOND);
        for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
          List<DefaultPubSubMessage> pubSubMessages = entry.getValue();
          for (DefaultPubSubMessage message: pubSubMessages) {
            if (!message.getKey().isControlMessage()) {
              messageCount += 1;
            }
          }
        }
        if (messageCount >= 2) {
          break;
        }
      }
      Assert.assertTrue(
          messageCount >= 2,
          "Oversized merged UPDATE should be split into at least two produced messages, but got " + messageCount);
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testPutOnlyWithBatchingEnabled() {
    final String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = getParentControllerUrl();
    VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("writecompute/test/PersonV1.avsc"));

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", STRING_SCHEMA.toString(), valueSchema.toString()));

      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setPartitionCount(1)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      veniceClusterWrapper.waitVersion(storeName, 1);
    }
    SystemProducer veniceProducer = null;
    VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    Pair<String, String> additionalConfig = new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_INTERVAL_MS, "1000");
    Pair<String, String> additionalConfig2 = new Pair<>(ConfigKeys.WRITER_BATCHING_MAX_BUFFER_SIZE_IN_BYTES, "1024000");
    veniceProducer = IntegrationTestPushUtils
        .getSamzaProducer(veniceCluster, storeName, Version.PushType.STREAM, additionalConfig, additionalConfig2);

    String key = "key";
    // Message 1: Will be compacted by Message 2
    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 99);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value);

    // Message 2: Should be produced.
    value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 100);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value);

    // Message 3: Should be produced (logical TS)
    value = new GenericData.Record(valueSchema);
    value.put("name", "val");
    value.put("age", 101);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key, value, 100L);

    String key2 = "key2";
    // Message 4: Will be compacted by Message 5
    value = new GenericData.Record(valueSchema);
    value.put("name", "DEN");
    value.put("age", 2023);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    // Message 5: Should be produced.
    value = new GenericData.Record(valueSchema);
    value.put("name", "CLE");
    value.put("age", 2024);
    sendStreamingRecordWithoutFlush(veniceProducer, storeName, key2, value);

    try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        try {
          GenericRecord retrievedValue = readValue(storeReader, key);
          assertNotNull(retrievedValue, "Key " + key + " should not be missing!");
          assertEquals(retrievedValue.get("name").toString(), "val");
          assertEquals(retrievedValue.get("age"), 100);

          retrievedValue = readValue(storeReader, key2);
          assertNotNull(retrievedValue, "Key " + key2 + " should not be missing!");
          assertEquals(retrievedValue.get("name").toString(), "CLE");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });

    } finally {
      veniceProducer.stop();
    }

    // Consume all the RT messages and validated how many data records were produced.
    PubSubBrokerWrapper pubSubBrokerWrapper =
        childDatacenters.get(0).getClusters().get(CLUSTER_NAME).getPubSubBrokerWrapper();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {

      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName, 1)), 0),
          PubSubSymbolicPosition.EARLIEST,
          false);
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(1000 * Time.MS_PER_SECOND);
      int messageCount = 0;
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: messages.entrySet()) {
        List<DefaultPubSubMessage> pubSubMessages = entry.getValue();
        for (DefaultPubSubMessage message: pubSubMessages) {
          if (!message.getKey().isControlMessage()) {
            messageCount += 1;
          }
        }
      }
      Assert.assertEquals(messageCount, 3);
    }
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

}
