package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
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
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.log4j.Logger;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;


/**
 * General-purpose utility functions for tests.
 */
public class TestUtils {
  private static final Logger LOGGER = Logger.getLogger(TestUtils.class);

  /** In milliseconds */
  private static final int WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = Time.MS_PER_SECOND / 5;
  private static final int MAX_WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = 60 * Time.MS_PER_SECOND;

  private final static InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  public static final String TEMP_DIRECTORY_SYSTEM_PROPERTY = "java.io.tmpdir";

  public static String getUniqueString() {
    return getUniqueString("");
  }

  public static String getUniqueString(String base) {
    /**
     * Dash(-) is an illegal character for avro; both compute and ETL feature uses store name in their avro schema;
     * so it's better not to use dash in the store name.
     */
    return base + "-" + System.nanoTime() + "-" + RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
  }

  public static String getUniqueAlphanumericString(String base) {
    return base + System.nanoTime() + RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
  }

  public static String getUniqueTempPath() {
    return getUniqueTempPath("");
  }

  public static String getUniqueTempPath(String prefix) {
    return Paths.get(System.getProperty(TEMP_DIRECTORY_SYSTEM_PROPERTY), TestUtils.getUniqueString(prefix)).toAbsolutePath().toString();
  }

  public static File getTempDataDirectory(Optional<String> name) {
    String tmpDirectory = System.getProperty(TestUtils.TEMP_DIRECTORY_SYSTEM_PROPERTY);
    String directoryName = TestUtils.getUniqueString(name.orElse("Venice-Data"));
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dir.mkdir();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {}
    }));
    return dir;
  }

  public static File getTempDataDirectory() {
    return getTempDataDirectory(Optional.empty());
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete.  Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param conditionToWaitFor A {@link BooleanSupplier} which should execute the non-deterministic action and
   *                           return true if it is successful, false otherwise.
   */
  public static void waitForNonDeterministicCompletion(long timeout, TimeUnit timeoutUnits, BooleanSupplier conditionToWaitFor) {
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    while (!conditionToWaitFor.getAsBoolean()) {
      if (System.currentTimeMillis() > timeoutTime) {
        throw new RuntimeException("Operation did not complete in time");
      }
      Utils.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
    }
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
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param assertionToWaitFor A {@link NonDeterministicAssertion} which should simply execute without exception
   *                           if it is successful, or throw an {@link AssertionError} otherwise.
   * @throws AssertionError throws the exception thrown by the {@link NonDeterministicAssertion} if the maximum
   *                        wait time has been exceeded.
   */
  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    waitForNonDeterministicAssertion(timeout, timeoutUnits, false, assertionToWaitFor);
  }

  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      boolean exponentialBackOff,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    waitForNonDeterministicAssertion(timeout, timeoutUnits, exponentialBackOff, false, assertionToWaitFor);
  }

  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      boolean exponentialBackOff,
                                                      boolean retryForThrowable,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    long waitTime = WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS;
    while (true) {
      try {
        assertionToWaitFor.execute();
        return;
      } catch (AssertionError | VerifyError e) {
        if (System.currentTimeMillis() > timeoutTime) {
          throw e;
        } else {
          LOGGER.info("waitForNonDeterministicAssertion caught an AssertionError. Will retry again in: " + waitTime
              + " ms. Assertion message: " + e.getMessage());
        }
      } catch (Throwable e) {
        if (!retryForThrowable || System.currentTimeMillis() > timeoutTime) {
          throw new AssertionError(e);
        } else {
          LOGGER.info("waitForNonDeterministicAssertion caught a Throwable. Will retry again in: " + waitTime
              + " ms. Assertion message: " + e.getMessage());
        }
      } finally {
        Utils.sleep(waitTime);
        if (exponentialBackOff) {
          waitTime = Math.max(Time.MS_PER_SECOND, Math.min(waitTime * 2, MAX_WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS));
        }
      }
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
      long timeout, TimeUnit timeoutUnits, Optional<Logger> logger) {
    waitForNonDeterministicCompletion(timeout, timeoutUnits, () -> {
      String emptyPushStatus = controllerClient.queryJobStatus(topicName, Optional.empty()).getStatus();
      boolean ignoreError = false;
      try {
        assertNotEquals(emptyPushStatus, ExecutionStatus.ERROR.toString(), "Unexpected empty push failure");
        ignoreError = true;
        assertEquals(emptyPushStatus, ExecutionStatus.COMPLETED.toString(), "Empty push is yet to complete");
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

  public interface NonDeterministicAssertion {
    void execute() throws Exception;
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
    offsetRecord.setOffset(currentOffset);
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
    protected String getKafkaZkAddress() {
      return kafkaZkAddress;
    }

    @Override
    public String getKafkaBootstrapServers() {
      return kafkaBootstrapServers;
    }

    @Override
    protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress) {
      return new TestKafkaClientFactory(kafkaBootstrapServers, kafkaZkAddress);
    }
  }

  public static Store getRandomStore() {
    return new ZKStore(TestUtils.getUniqueString("RandomStore"),
        TestUtils.getUniqueString("RandomOwner"),
        System.currentTimeMillis(),
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        3); // TODO: figure out how to get hold of a sensible RF value here
  }

  public static <T extends ControllerResponse> T assertCommand(T response) {
    Assert.assertFalse(response.isError(), "Controller error: " + response.getError());
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
        String message = "System exit requested with error " + status;
        SecurityException e = new SecurityException(message);
        LOGGER.info("checkExit called", e);
        throw e;
      }
    });
  }

  public static void restoreSystemExit() {
    System.setSecurityManager(null);
  }
}
