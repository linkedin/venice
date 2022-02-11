package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixInstanceConverter;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.SharedKafkaProducerService;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import io.tehuti.metrics.MetricsRepository;

import java.security.Permission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


/**
 * General-purpose utility functions for tests.
 */
public class TestUtils {
  private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

  /** In milliseconds */
  private static final long ND_ASSERTION_MIN_WAIT_TIME_MS = 100;
  private static final long ND_ASSERTION_MAX_WAIT_TIME_MS = 3000;

  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  public static String dequalifyClassName(String className) {
    return className.substring(className.lastIndexOf('.') + 1);
  }

  private static String getCallingMethod() {
    return Arrays.stream(Thread.currentThread().getStackTrace())
        .filter(frame -> frame.getClassName().startsWith("com.linkedin.") &&
                             !frame.getClassName().equals(TestUtils.class.getName()))
        .findFirst()
        .map(frame -> String.format("%s.%s.%d", dequalifyClassName(frame.getClassName()), frame.getMethodName(), frame.getLineNumber()))
        .orElse("UNKNOWN_METHOD");
  }
  /**
   * To be used for tests when we need to wait for an async operation to complete.  Pass a timeout, and a lambda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnit {@link TimeUnit} for the {@param timeout}
   * @param condition A {@link BooleanSupplier} which should execute the non-deterministic action and
   *                           return true if it is successful, false otherwise.
   */
  public static void waitForNonDeterministicCompletion(long timeout, TimeUnit timeoutUnit, BooleanSupplier condition) throws AssertionError {
    long startTimeMs = System.currentTimeMillis();
    long nextDelayMs = ND_ASSERTION_MIN_WAIT_TIME_MS;
    long deadlineMs = startTimeMs + timeoutUnit.toMillis(timeout);
    try {
      while (!condition.getAsBoolean()) {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        assertTrue(remainingMs > nextDelayMs, "Non-deterministic condition not met.");
        assertTrue(Utils.sleep(nextDelayMs), "Waiting for non-deterministic condition was interrupted.");
      }
    } finally {
      LOGGER.info("{} waiting took {} ms.", getCallingMethod(), System.currentTimeMillis() - startTimeMs);
    }
  }

  public interface NonDeterministicAssertion {
    void execute() throws Exception;
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete. Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * There is an issue within Mockito where it emits VerifyError instead of ArgumentsAreDifferent Exception.
   * Check out "ExceptionFactory#JunitArgsAreDifferent" for details. The workaround here is to catch both
   * assert and verify error.
   * TODO: find a better way resolve it
   *
   * @param timeout amount of time to wait
   * @param timeoutUnit {@link TimeUnit} for the {@param timeout}
   * @param assertion A {@link NonDeterministicAssertion} which should simply execute without exception
   *                           if it is successful, or throw an {@link AssertionError} otherwise.
   * @throws AssertionError throws the exception thrown by the {@link NonDeterministicAssertion} if the maximum
   *                        wait time has been exceeded.
   */
  public static void waitForNonDeterministicAssertion(
      long timeout,
      TimeUnit timeoutUnit,
      NonDeterministicAssertion assertion) throws AssertionError {
    waitForNonDeterministicAssertion(timeout, timeoutUnit, false, assertion);
  }

  public static void waitForNonDeterministicAssertion(
      long timeout,
      TimeUnit timeoutUnit,
      boolean exponentialBackOff,
      NonDeterministicAssertion assertion) throws AssertionError {
    waitForNonDeterministicAssertion(timeout, timeoutUnit, exponentialBackOff, false, assertion);
  }

  public static void waitForNonDeterministicAssertion(
      long timeout,
      TimeUnit timeoutUnit,
      boolean exponentialBackOff,
      boolean retryOnThrowable,
      NonDeterministicAssertion assertion) throws AssertionError {
    long startTimeMs = System.currentTimeMillis();
    long nextDelayMs = ND_ASSERTION_MIN_WAIT_TIME_MS;
    long deadlineMs = startTimeMs + timeoutUnit.toMillis(timeout);
    try {
      for (;;) {
        try {
          assertion.execute();
          return;
        } catch (Throwable e) {
          long remainingMs = deadlineMs - System.currentTimeMillis();
          if (remainingMs < nextDelayMs || !(retryOnThrowable || e instanceof AssertionError)) {
            throw (e instanceof AssertionError ? (AssertionError) e : new AssertionError(e));
          }
          LOGGER.info("Non-deterministic assertion not met: {}. Will retry again in {} ms.", e, nextDelayMs);
          assertTrue(Utils.sleep(nextDelayMs), "Waiting for non-deterministic assertion was interrupted.");
          if (exponentialBackOff) {
            nextDelayMs = Math.min(nextDelayMs * 2, remainingMs - nextDelayMs);
            nextDelayMs = Math.min(nextDelayMs, ND_ASSERTION_MAX_WAIT_TIME_MS);
          }
        }
      }
    } finally {
      LOGGER.info("{} waiting took {} ms.", getCallingMethod(), System.currentTimeMillis() - startTimeMs);
    }
  }

  public static VersionCreationResponse createVersionWithBatchData(ControllerClient controllerClient, String storeName,
      String keySchema, String valueSchema, Stream<Map.Entry> batchData) throws Exception {
    return createVersionWithBatchData(controllerClient, storeName, keySchema, valueSchema, batchData,
        HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
  }

  public static VersionCreationResponse createVersionWithBatchData(ControllerClient controllerClient, String storeName,
      String keySchema, String valueSchema, Stream<Map.Entry> batchData, int valueSchemaId) throws Exception {
    VersionCreationResponse response = controllerClient.requestTopicForWrites(
        storeName,
        1024,
        Version.PushType.BATCH,
        Version.guidBasedDummyPushId(),
        true,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    assertFalse(response.isError());
    writeBatchData(response, keySchema, valueSchema, batchData, valueSchemaId);
    return response;
  }

  public static void writeBatchData(VersionCreationResponse response, String keySchema, String valueSchema,
      Stream<Map.Entry> batchData, int valueSchemaId) throws Exception {
    Properties props = new Properties();
    props.put(KAFKA_BOOTSTRAP_SERVERS, response.getKafkaBootstrapServers());
    props.setProperty(PARTITIONER_CLASS, response.getPartitionerClass());
    props.putAll(response.getPartitionerParams());
    props.setProperty(AMPLIFICATION_FACTOR, String.valueOf(response.getAmplificationFactor()));
    VeniceWriterFactory writerFactory = TestUtils.getVeniceWriterFactory(props);

    String kafkaTopic = response.getKafkaTopic();
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(response.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        response.getPartitionerClass(),
        response.getAmplificationFactor(),
        new VeniceProperties(partitionerProperties));
    try (
        VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchema);
        VeniceWriter<Object, Object, byte[]> writer = writerFactory.createVeniceWriter(kafkaTopic, keySerializer, valueSerializer, venicePartitioner)) {

      writer.broadcastStartOfPush(Collections.emptyMap());
      for (Map.Entry e : (Iterable<Map.Entry>) batchData::iterator) {
        writer.put(e.getKey(), e.getValue(), valueSchemaId).get();
      }
      writer.broadcastEndOfPush(Collections.emptyMap());
    }
  }

  /**
   * Wait for the push job for a store version or topic to be completed. The polling will fast fail if the push is
   * found to be in ERROR state.
   */
  public static void waitForNonDeterministicPushCompletion(String topicName, ControllerClient controllerClient,
      long timeout, TimeUnit timeoutUnit, Optional<Logger> logger) {
    waitForNonDeterministicCompletion(timeout, timeoutUnit, () -> {
      String emptyPushStatus = controllerClient.queryJobStatus(topicName, Optional.empty()).getStatus();
      boolean ignoreError = false;
      try {
        assertNotEquals(emptyPushStatus, ExecutionStatus.ERROR.toString(), "Unexpected push failure");
        ignoreError = true;
        assertEquals(emptyPushStatus, ExecutionStatus.COMPLETED.toString(), "Push is yet to complete");
        return true;
      } catch (AssertionError | VerifyError e) {
        if (ignoreError) {
          logger.ifPresent(value -> value.info(e.getMessage()));
          return false;
        }
        throw e;
      }
    });
  }

  public static void waitForNonDeterministicIncrementalPushCompletion(String topicName, String incrementalPushVersion,
      ControllerClient controllerClient, long timeout, TimeUnit timeoutUnit, Optional<Logger> logger) {
    waitForNonDeterministicCompletion(timeout, timeoutUnit, () -> {
      String emptyPushStatus = controllerClient.queryJobStatus(topicName, Optional.of(incrementalPushVersion)).getStatus();
      boolean ignoreError = false;
      try {
        assertNotEquals(emptyPushStatus, ExecutionStatus.ERROR.toString(), "Unexpected incremental push failure");
        ignoreError = true;
        assertEquals(emptyPushStatus, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.toString(),
            "Incremental push is yet to complete");
        return true;
      } catch (AssertionError | VerifyError e) {
        if (ignoreError) {
          logger.ifPresent(value -> value.info(e.getMessage()));
          return false;
        }
        throw e;
      }
    });
  }

  public static Store createTestStore(String name, String owner, long createdTime) {
      Store store = new ZKStore(name, owner, createdTime, PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH,
          ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS,
          3); // TODO: figure out how to get hold of a sensible RF value here
      // Set the default timestamp to make sure every creation will return the same Store object.
      store.setLatestVersionPromoteToCurrentTimestamp(-1);
      return store;
  }

  /**
   * @deprecated
   * TODO: migrate to use {@link ServiceFactory} for generating a participant
   * */
  @Deprecated
  public static SafeHelixManager getParticipant(String cluster, String nodeId, String zkAddress, int httpPort, String stateModelDef) {
    MockTestStateModelFactory stateModelFactory = new MockTestStateModelFactory();
    return getParticipant(cluster, nodeId, zkAddress, httpPort, stateModelFactory, stateModelDef);
  }

  public static SafeHelixManager getParticipant(String cluster, String nodeId, String zkAddress, int httpPort,
      StateModelFactory<StateModel> stateModelFactory, String stateModelDef) {
    SafeHelixManager participant = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(cluster, nodeId, InstanceType.PARTICIPANT, zkAddress));
    participant.getStateMachineEngine()
        .registerStateModelFactory(stateModelDef,
            stateModelFactory);
    participant.setLiveInstanceInfoProvider(
        () -> HelixInstanceConverter.convertInstanceToZNRecord(new Instance(nodeId, Utils.getHostName(), httpPort)));
    return participant;
  }

  public static OffsetRecord getOffsetRecord(long currentOffset) {
    return getOffsetRecord(currentOffset, Optional.empty());
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, boolean complete) {
    return getOffsetRecord(currentOffset, complete ? Optional.of(1000L) : Optional.of(0L));
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, Optional<Long> endOfPushOffset) {
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    offsetRecord.setCheckpointLocalVersionTopicOffset(currentOffset);
    if (endOfPushOffset.isPresent()) {
      offsetRecord.endOfPushReceived(endOfPushOffset.get());
    }
    return offsetRecord;
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, boolean complete, String incrementalPushVersion) {
    OffsetRecord offsetRecord = getOffsetRecord(currentOffset, complete);
    IncrementalPush incrementalPush = new IncrementalPush();
    incrementalPush.version = incrementalPushVersion;
    offsetRecord.setIncrementalPush(incrementalPush);
    return offsetRecord;
  }

  public static VeniceControllerMultiClusterConfig getMultiClusterConfigFromOneCluster(
      VeniceControllerConfig controllerConfig) {
    Map<String,VeniceControllerConfig> configMap = new HashMap<>();
    configMap.put(controllerConfig.getClusterName(),controllerConfig);
    return new VeniceControllerMultiClusterConfig(configMap);
  }

  public static String getClusterToDefaultD2String(String cluster) {
    return cluster + ":" + D2TestUtils.DEFAULT_TEST_SERVICE_NAME;
  }

  public static VeniceWriterFactory getVeniceWriterFactory(String kafkaBootstrapServers) {
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    return getVeniceWriterFactory(properties);
  }

  public static VeniceWriterFactory getVeniceWriterFactory(Properties properties) {
    Properties factoryProperties = new Properties();
    factoryProperties.put(KAFKA_REQUEST_TIMEOUT_MS, 5000);
    factoryProperties.put(KAFKA_DELIVERY_TIMEOUT_MS, 5000);
    factoryProperties.putAll(properties);
    return new VeniceWriterFactory(factoryProperties);
  }

  public static SharedKafkaProducerService getSharedKafkaProducerService(Properties properties) {
    Properties factoryProperties = new Properties();
    factoryProperties.put(KAFKA_REQUEST_TIMEOUT_MS, 5000);
    factoryProperties.put(KAFKA_DELIVERY_TIMEOUT_MS, 5000);
    factoryProperties.putAll(properties);
    SharedKafkaProducerService sharedKafkaProducerService =
        new SharedKafkaProducerService(factoryProperties, 1, new SharedKafkaProducerService.KafkaProducerSupplier() {
          @Override
          public KafkaProducerWrapper getNewProducer(VeniceProperties props) {
            return new ApacheKafkaProducer(props);
          }
        }, new MetricsRepository(), new HashSet<>(Arrays.asList("outgoing-byte-rate",
            "record-send-rate","batch-size-max","batch-size-avg","buffer-available-bytes","buffer-exhausted-rate")));
    return sharedKafkaProducerService;
  }

  public static VeniceWriterFactory getVeniceWriterFactoryWithSharedProducer(Properties properties,
      Optional<SharedKafkaProducerService> sharedKafkaProducerService) {
    Properties factoryProperties = new Properties();
    factoryProperties.putAll(properties);
    return new VeniceWriterFactory(factoryProperties, sharedKafkaProducerService);
  }

  public static KafkaClientFactory getVeniceConsumerFactory(KafkaBrokerWrapper kafka) {
    return new TestKafkaClientFactory(kafka.getAddress(), kafka.getZkAddress());
  }

  private static class TestKafkaClientFactory extends KafkaClientFactory {
    private final String kafkaBootstrapServers;
    private final String kafkaZkAddress;
    public TestKafkaClientFactory(String kafkaBootstrapServers, String kafkaZkAddress) {
      this.kafkaBootstrapServers = kafkaBootstrapServers;
      this.kafkaZkAddress = kafkaZkAddress;
    }

    @Override
    public Properties setupSSL(Properties properties) {
      properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
      return properties;
    }

    @Override
    protected String getKafkaAdminClass() {
      return KafkaAdminClient.class.getName();
    }

    @Override
    protected String getWriteOnlyAdminClass() {
      return getKafkaAdminClass();
    }

    @Override
    protected String getReadOnlyAdminClass() {
      return getKafkaAdminClass();
    }

    @Override
    protected String getKafkaZkAddress() {
      return kafkaZkAddress;
    }

    @Override
    public String getKafkaBootstrapServers() {
      return kafkaBootstrapServers;
    }

    @Override
    protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress, Optional<MetricsParameters> metricsParameters) {
      return new TestKafkaClientFactory(kafkaBootstrapServers, kafkaZkAddress);
    }
  }

  public static Store getRandomStore() {
    return new ZKStore(Utils.getUniqueString("RandomStore"),
        Utils.getUniqueString("RandomOwner"),
        System.currentTimeMillis(),
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        3); // TODO: figure out how to get hold of a sensible RF value here
  }

  public static <T extends ControllerResponse> T assertCommand(T response) {
    return assertCommand(response, "Controller error");
  }

  public static <T extends ControllerResponse> T assertCommand(T response, String assertionErrorMessage) {
    Assert.assertFalse(response.isError(), assertionErrorMessage + ": " + response.getError());
    return response;
  }

  public static void preventSystemExit() {
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPermission(Permission perm) {}

      @Override
      public void checkPermission(Permission perm, Object context) {}

      @Override
      public void checkExit(int status) {
        if (status != 0) {
          String message = "System exit requested with error " + status;
          SecurityException e = new SecurityException(message);
          LOGGER.info("checkExit called", e);
          throw e;
        }
      }
    });
  }

  public static void restoreSystemExit() {
    System.setSecurityManager(null);
  }

  public static void createAndVerifyStoreInAllRegions(String storeName, ControllerClient parentControllerClient, List<ControllerClient> controllerClientList) {
    Assert.assertFalse(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA).isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      for (ControllerClient client : controllerClientList) {
        Assert.assertFalse(client.getStore(storeName).isError());
      }
    });
  }

  public static void verifyDCConfigNativeAndActiveRepl(ControllerClient controllerClient, String storeName, boolean enabledNR, boolean enabledAA) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      Assert.assertEquals(storeResponse.getStore().isNativeReplicationEnabled(), enabledNR, "The native replication config does not match.");
      Assert.assertEquals(storeResponse.getStore().isActiveActiveReplicationEnabled(), enabledAA, "The active active replication config does not match.");
    });
  }
}
