package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.annotations.Test;


/**
 * Replays a physically compacted hybrid VT on a fresh server while its completed push is still
 * awaiting a version swap. The original EOP/prc is retained; the test never fabricates its count.
 */
public class TestCompactedBatchPushRecordCount {
  private static final int RECORD_COUNT = 50;

  @Test(timeOut = 180_000)
  public void testFreshBootstrapOfCompactedDeferredVersion() throws Exception {
    Utils.thisIsLocalhost();
    Map<String, String> brokerConfig = new HashMap<>();
    // The normal integration broker disables its cleaner. Enable it for this test only.
    brokerConfig.put("log.cleaner.enable", "true");
    brokerConfig.put("log.cleaner.backoff.ms", "100");
    brokerConfig.put("log.segment.bytes", "1048576");
    brokerConfig.put("log.cleaner.min.cleanable.ratio", "0.01");
    Properties serverConfig = new Properties();
    serverConfig.put(ConfigKeys.SERVER_UNIQUE_INGESTED_KEY_COUNT_HLL_ENABLED, true);
    serverConfig.put(ConfigKeys.SERVER_BATCH_PUSH_RECORD_COUNT_VERIFICATION_FAIL_ON_MISMATCH_ENABLED, true);

    try (
        PubSubBrokerWrapper broker = ServiceFactory.getPubSubBroker(
            new PubSubBrokerConfigs.Builder().setRegionName("standalone")
                .setAdditionalBrokerConfiguration(brokerConfig)
                .build());
        VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(
            new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
                .numberOfServers(1)
                .numberOfRouters(1)
                .replicationFactor(1)
                .sslToKafka(false)
                .sslToStorageNodes(false)
                .kafkaBrokerWrapper(broker)
                .extraProperties(serverConfig)
                .build());
        ControllerClient controller = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
      String store = Utils.getUniqueString("compacted_deferred_prc");
      cluster.getNewStore(store);
      TestUtils.assertCommand(
          controller.updateStore(
              store,
              new UpdateStoreQueryParams().setPartitionCount(1)
                  .setHybridRewindSeconds(1)
                  .setHybridOffsetLagThreshold(1)));
      VersionCreationResponse version = TestUtils.assertCommand(
          controller.requestTopicForWrites(
              store,
              1024,
              Version.PushType.BATCH,
              Utils.getUniqueString("push"),
              false,
              false,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1,
              true));
      String topic = version.getKafkaTopic();
      VeniceServerWrapper originalServer = cluster.getVeniceServers().get(0);

      try (VeniceWriter<String, String, byte[]> writer = cluster.getVeniceWriter(topic)) {
        writer.broadcastStartOfPush(Collections.emptyMap());
        for (int i = 0; i < RECORD_COUNT; i++) {
          writer.put("key_" + i, "batch_" + i, 1).get();
        }
        writer.broadcastEndOfPush(Collections.emptyMap(), Collections.singletonMap(0, (long) RECORD_COUNT));
        writer.flush();
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          assertEquals(controller.queryJobStatus(topic).getStatus(), ExecutionStatus.COMPLETED.name());
          assertTrue(controller.getStore(store).getStore().getCurrentVersion() < version.getVersion());
          assertTrue(
              controller.getStore(store).getStore().getVersion(version.getVersion()).get().isVersionSwapDeferred());
          assertComplete(originalServer, topic);
        });

        // Write the post-EOP VT updates directly, as an ingestion leader would. This deliberately
        // isolates VT replay from RT replication; these are real Venice records on the real broker.
        for (int i = 0; i < RECORD_COUNT; i++) {
          writer.put("key_" + i, "updated_" + i, 1).get();
        }
        // Force a size-based roll so the cleaner can process the preceding update segments.
        // Incompressible padding is needed: repeated characters compress below the segment size.
        byte[] paddingBytes = new byte[262144];
        new Random(0).nextBytes(paddingBytes);
        String padding = Base64.getEncoder().encodeToString(paddingBytes);
        for (int i = 0; i < 12; i++) {
          writer.put("roll_marker", padding, 1).get();
        }
        writer.flush();
      }

      Properties kafkaProperties = new Properties();
      kafkaProperties.put("bootstrap.servers", broker.getAddress());
      try (AdminClient admin = AdminClient.create(kafkaProperties)) {
        admin.alterConfigs(
            Collections.singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, topic),
                new Config(
                    Arrays.asList(
                        new ConfigEntry("cleanup.policy", "compact"),
                        new ConfigEntry("min.compaction.lag.ms", "0"),
                        new ConfigEntry("min.cleanable.dirty.ratio", "0.01")))))
            .all()
            .get(30, TimeUnit.SECONDS);
      }

      // Do not confuse policy enablement with physical compaction: re-read EARLIEST through the
      // broker's end offset until fewer batch PUTs survive BEFORE the unchanged original EOP.
      kafkaProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
      kafkaProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
      kafkaProperties.put("enable.auto.commit", "false");
      TopicPartition partition = new TopicPartition(topic, 0);
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProperties);
          KafkaValueSerializer valueSerializer = new KafkaValueSerializer()) {
        consumer.assign(Collections.singleton(partition));
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          consumer.seekToBeginning(Collections.singleton(partition));
          long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
          int survivingBatchRecords = 0;
          boolean sawEop = false;
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
          while (consumer.position(partition) < endOffset && System.nanoTime() < deadline) {
            for (ConsumerRecord<byte[], byte[]> record: consumer.poll(Duration.ofMillis(100))) {
              KafkaMessageEnvelope envelope = valueSerializer.deserialize(topic, record.value());
              if (MessageType.valueOf(envelope) == MessageType.CONTROL_MESSAGE && ControlMessageType
                  .valueOf((ControlMessage) envelope.payloadUnion) == ControlMessageType.END_OF_PUSH) {
                sawEop = true;
                assertEquals(
                    ByteBuffer.wrap(
                        record.headers().lastHeader(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER).value())
                        .getLong(),
                    (long) RECORD_COUNT);
              } else if (!sawEop && MessageType.valueOf(envelope) == MessageType.PUT) {
                survivingBatchRecords++;
              }
            }
          }
          assertTrue(sawEop, "Original EOP must survive compaction");
          assertTrue(survivingBatchRecords < RECORD_COUNT / 2, "Wait for evidence of actual cleaner removal");
        });
      }

      cluster.stopVeniceServer(originalServer.getPort());
      VeniceServerWrapper freshServer = cluster.addVeniceServer(new Properties(), serverConfig);
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        assertTrue(controller.getStore(store).getStore().getCurrentVersion() < version.getVersion());
        PartitionConsumptionState pcs = assertComplete(freshServer, topic);
        assertTrue(pcs.getBatchPushRecordCount() < RECORD_COUNT / 2, "Fresh bootstrap must replay compacted batch");
        // Unlike the batch counter, HLL continues tracking post-EOP updates, so its value after
        // catch-up is not the estimate seen at EOP. Both verifiers run enabled in this test.
      });

      // Swap only after proving successful bootstrap in FUTURE; verify latest-value read semantics
      // through the real router/client rather than treating a successful EOP as sufficient.
      TestUtils.assertCommand(
          controller.updateStore(store, new UpdateStoreQueryParams().setCurrentVersion(version.getVersion())));
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(store).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          for (int i = 0; i < RECORD_COUNT; i++) {
            assertEquals(client.get("key_" + i).get().toString(), "updated_" + i);
          }
        });
      }
    }
  }

  private static PartitionConsumptionState assertComplete(VeniceServerWrapper server, String topic) {
    StoreIngestionTask task = server.getVeniceServer().getKafkaStoreIngestionService().getStoreIngestionTask(topic);
    assertNotNull(task);
    PartitionConsumptionState pcs = task.getPartitionConsumptionState(0);
    assertNotNull(pcs);
    assertFalse(pcs.isErrorReported(), "Replay must not report a fatal record-count mismatch");
    assertTrue(pcs.isComplete(), "Fresh hybrid replica must complete EOP and catch up");
    return pcs;
  }
}
