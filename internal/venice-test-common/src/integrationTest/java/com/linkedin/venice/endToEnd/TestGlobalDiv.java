package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.utils.Utils.*;

import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.GlobalDivState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestGlobalDiv {
  private static final Logger LOGGER = LogManager.getLogger(TestGlobalDiv.class);

  private VeniceClusterWrapper sharedVenice;

  SecureRandom random = new SecureRandom();

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));

    // N.B.: RF 2 with 3 servers is important, in order to test both the leader and follower code paths
    sharedVenice = ServiceFactory.getVeniceCluster(1, 0, 0, 2, 1000000, false, false, extraProperties);

    Properties routerProperties = new Properties();

    sharedVenice.addVeniceRouter(routerProperties);
    // Added a server with shared consumer enabled.
    Properties serverPropertiesWithSharedConsumer = new Properties();
    serverPropertiesWithSharedConsumer.setProperty(SSL_TO_KAFKA_LEGACY, "false");
    extraProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4");
    extraProperties.setProperty(
        SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
        KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    // Enable global div feature in the integration test.
    extraProperties.setProperty(SERVER_GLOBAL_RT_DIV_ENABLED, "true");

    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    sharedVenice.addVeniceServer(serverPropertiesWithSharedConsumer, extraProperties);
    LOGGER.info("Finished creating VeniceClusterWrapper");
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
  }

  /**
   * This test verifies functionality of sending chunked/non-chunked div messages:
   *
   * 1. Create a hybrid store and create a store version.
   * 2. Send a non-chunked div message to the version topic.
   * 3. Send a chunked div message to the version topic.
   * 4. Verify the messages are sent successfully.
   * 5. TODO: Add more verification steps on the server side later.
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testChunkedDiv() {
    String storeName = Utils.getUniqueString("store");
    final int partitionCount = 1;
    final int keyCount = 10;

    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
        .setHybridRewindSeconds(2000000)
        .setHybridOffsetLagThreshold(10)
        .setPartitionCount(partitionCount);

    sharedVenice.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });

    // Create store version 1 by writing keyCount records.
    sharedVenice.createVersion(
        storeName,
        DEFAULT_KEY_SCHEMA,
        DEFAULT_VALUE_SCHEMA,
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));

    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, sharedVenice.getPubSubBrokerWrapper().getAddress());

    // Set max segment elapsed time to 0 to enforce creating small segments aggressively
    veniceWriterProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, "0");
    veniceWriterProperties.putAll(
        PubSubBrokerWrapper
            .getBrokerDetailsForClients(Collections.singletonList(sharedVenice.getPubSubBrokerWrapper())));
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        sharedVenice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

    try (VeniceWriter<byte[], byte[], byte[]> verstionTopicWriter =
        TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
            .createVeniceWriter(new VeniceWriterOptions.Builder(Version.composeKafkaTopic(storeName, 1)).build())) {

      InternalAvroSpecificSerializer<GlobalDivState> serializer =
          AvroProtocolDefinition.GLOBAL_DIV_STATE.getSerializer();

      GlobalDivState state = createGlobalDivState("localhost:9090", false);
      verstionTopicWriter
          .sendChunkSupportedDivMessage(
              0,
              "NonChunkedKey".getBytes(),
              ByteUtils.extractByteArray(serializer.serialize(state)))
          .get();
      LOGGER.info("Sent non-chunked div message");

      state = createGlobalDivState("localhost:9092", true);
      verstionTopicWriter
          .sendChunkSupportedDivMessage(
              0,
              "ChunkedKey".getBytes(),
              ByteUtils.extractByteArray(serializer.serialize(state)))
          .get();
      LOGGER.info("Sent chunked div message");

      // TODO: Add more verification steps later.
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private GlobalDivState createGlobalDivState(String srcUrl, boolean isChunked) {
    GlobalDivState state = new GlobalDivState();
    state.producerStates = new HashMap<>();
    state.setSrcUrl(srcUrl);

    if (isChunked) {
      // Create a large state with 20k entries.
      for (int i = 0; i < 20000; i++) {
        byte[] bytes = new byte[256];
        random.nextBytes(bytes);
        GUID guid = new GUID(bytes);
        state.producerStates.put(guidToUtf8(guid), createProducerPartitionState(i, i));
      }
    } else {
      state.producerStates = Collections.emptyMap();
    }
    return state;
  }

  private CharSequence guidToUtf8(GUID guid) {
    return new Utf8(GuidUtils.getCharSequenceFromGuid(guid));
  }

  private ProducerPartitionState createProducerPartitionState(int segment, int sequence) {
    ProducerPartitionState ppState = new ProducerPartitionState();
    ppState.segmentNumber = segment;
    ppState.segmentStatus = SegmentStatus.IN_PROGRESS.getValue();
    ppState.messageSequenceNumber = sequence;
    ppState.messageTimestamp = System.currentTimeMillis();
    ppState.checksumType = CheckSumType.NONE.getValue();
    ppState.checksumState = ByteBuffer.allocate(0);
    ppState.aggregates = new HashMap<>();
    ppState.debugInfo = new HashMap<>();
    return ppState;
  }
}
