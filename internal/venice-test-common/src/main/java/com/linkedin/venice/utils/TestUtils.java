package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.ConfigKeys.SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_MODE;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.Utils.getUniqueString;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.AggKafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.StoreBufferService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTaskFactory;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.GzipCompressor;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixInstanceConverter;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;


/**
 * General-purpose utility functions for tests.
 */
public class TestUtils {
  private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

  /** In milliseconds */
  private static final long ND_ASSERTION_MIN_WAIT_TIME_MS = 100;
  private static final long ND_ASSERTION_MAX_WAIT_TIME_MS = 3000;

  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  public static String dequalifyClassName(String className) {
    return className.substring(className.lastIndexOf('.') + 1);
  }

  private static String getCallingMethod() {
    return Arrays.stream(Thread.currentThread().getStackTrace())
        .filter(
            frame -> frame.getClassName().startsWith("com.linkedin.")
                && !frame.getClassName().equals(TestUtils.class.getName()))
        .findFirst()
        .map(
            frame -> String.format(
                "%s.%s.%d",
                dequalifyClassName(frame.getClassName()),
                frame.getMethodName(),
                frame.getLineNumber()))
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
  public static void waitForNonDeterministicCompletion(long timeout, TimeUnit timeoutUnit, BooleanSupplier condition)
      throws AssertionError {
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

  public static ControllerResponse updateStoreToHybrid(
      String storeName,
      ControllerClient parentControllerClient,
      Optional<Boolean> enableNativeReplication,
      Optional<Boolean> enableActiveActiveReplication,
      Optional<Boolean> enableChunking) {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L);

    enableNativeReplication.ifPresent(params::setNativeReplicationEnabled);
    enableActiveActiveReplication.ifPresent(params::setActiveActiveReplicationEnabled);
    enableChunking.ifPresent(params::setChunkingEnabled);

    return assertCommand(parentControllerClient.updateStore(storeName, params));
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

  public static VersionCreationResponse createVersionWithBatchData(
      ControllerClient controllerClient,
      String storeName,
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData) {
    return createVersionWithBatchData(
        controllerClient,
        storeName,
        keySchema,
        valueSchema,
        batchData,
        HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
  }

  public static VersionCreationResponse createVersionWithBatchData(
      ControllerClient controllerClient,
      String storeName,
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData,
      int valueSchemaId) {
    VersionCreationResponse response = TestUtils.assertCommand(
        controllerClient.requestTopicForWrites(
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
            -1));
    writeBatchData(response, keySchema, valueSchema, batchData, valueSchemaId);
    return response;
  }

  public static void writeBatchData(
      VersionCreationResponse response,
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData,
      int valueSchemaId) {
    writeBatchData(response, keySchema, valueSchema, batchData, valueSchemaId, CompressionStrategy.NO_OP, null);
  }

  public static void writeBatchData(
      VersionCreationResponse response,
      String keySchema,
      String valueSchema,
      Stream<Map.Entry> batchData,
      int valueSchemaId,
      CompressionStrategy compressionStrategy,
      Function<String, ByteBuffer> compressionDictionaryGenerator) {
    Properties props = new Properties();
    props.put(KAFKA_BOOTSTRAP_SERVERS, response.getKafkaBootstrapServers());
    props.setProperty(PARTITIONER_CLASS, response.getPartitionerClass());
    props.putAll(response.getPartitionerParams());
    props.setProperty(AMPLIFICATION_FACTOR, String.valueOf(response.getAmplificationFactor()));
    VeniceWriterFactory writerFactory = TestUtils.getVeniceWriterFactory(props);

    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(response.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        response.getPartitionerClass(),
        response.getAmplificationFactor(),
        new VeniceProperties(partitionerProperties));

    if (compressionStrategy != CompressionStrategy.NO_OP) {
      writeCompressed(
          writerFactory,
          keySchema,
          valueSchema,
          valueSchemaId,
          response.getKafkaTopic(),
          response.getPartitions() * response.getAmplificationFactor(),
          venicePartitioner,
          batchData,
          compressionStrategy,
          compressionDictionaryGenerator.apply(response.getKafkaTopic()));
    } else {
      writeUncompressed(
          writerFactory,
          keySchema,
          valueSchema,
          valueSchemaId,
          response.getKafkaTopic(),
          response.getPartitions() * response.getAmplificationFactor(),
          venicePartitioner,
          batchData);
    }
  }

  private static void writeCompressed(
      VeniceWriterFactory writerFactory,
      String keySchema,
      String valueSchema,
      int valueSchemaId,
      String kafkaTopic,
      int partitionCount,
      VenicePartitioner venicePartitioner,
      Stream<Map.Entry> batchData,
      CompressionStrategy compressionStrategy,
      ByteBuffer compressionDictionary) {
    VeniceCompressor compressor = null;
    if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      compressor = new ZstdWithDictCompressor(compressionDictionary.array(), Zstd.maxCompressionLevel());
    } else if (compressionStrategy == CompressionStrategy.GZIP) {
      compressor = new GzipCompressor();
    } else {
      compressor = new NoopCompressor();
    }
    try (VeniceWriter<byte[], byte[], byte[]> writer = writerFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(kafkaTopic).setPartitionCount(partitionCount)
            .setPartitioner(venicePartitioner)
            .build())) {
      writer.broadcastStartOfPush(
          false,
          false,
          compressionStrategy,
          Optional.ofNullable(compressionDictionary),
          Collections.emptyMap());
      VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
      VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchema);
      LinkedList<Future> putFutures = new LinkedList<>();
      for (Map.Entry e: (Iterable<Map.Entry>) batchData::iterator) {
        byte[] key = keySerializer.serialize(kafkaTopic, e.getKey());
        byte[] value = valueSerializer.serialize(kafkaTopic, e.getValue());
        value = compressor.compress(value);
        putFutures.add(writer.put(key, value, valueSchemaId));
      }
      for (Future future: putFutures) {
        future.get();
      }
      writer.broadcastEndOfPush(Collections.emptyMap());
    } catch (InterruptedException | ExecutionException | IOException e) {
      throw new VeniceException(e);
    }
  }

  private static void writeUncompressed(
      VeniceWriterFactory writerFactory,
      String keySchema,
      String valueSchema,
      int valueSchemaId,
      String kafkaTopic,
      int partitionCount,
      VenicePartitioner venicePartitioner,
      Stream<Map.Entry> batchData) {

    try (VeniceWriter<Object, Object, byte[]> writer = writerFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(kafkaTopic).setKeySerializer(new VeniceAvroKafkaSerializer(keySchema))
            .setValueSerializer(new VeniceAvroKafkaSerializer(valueSchema))
            .setPartitionCount(partitionCount)
            .setPartitioner(venicePartitioner)
            .build())) {
      writer.broadcastStartOfPush(Collections.emptyMap());

      LinkedList<Future> putFutures = new LinkedList<>();
      for (Map.Entry e: (Iterable<Map.Entry>) batchData::iterator) {
        putFutures.add(writer.put(e.getKey(), e.getValue(), valueSchemaId));
      }
      for (Future future: putFutures) {
        future.get();
      }
      writer.broadcastEndOfPush(Collections.emptyMap());
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException(e);
    }
  }

  /**
   * Wait for the push job for a store version or topic to be completed. The polling will fast fail if the push is
   * found to be in ERROR state.
   */
  public static void waitForNonDeterministicPushCompletion(
      String topicName,
      ControllerClient controllerClient,
      long timeout,
      TimeUnit timeoutUnit) {
    waitForNonDeterministicAssertion(timeout, timeoutUnit, () -> {
      JobStatusQueryResponse jobStatusQueryResponse =
          assertCommand(controllerClient.queryJobStatus(topicName, Optional.empty()));
      ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
      if (executionStatus == ExecutionStatus.ERROR) {
        throw new VeniceException("Unexpected push failure for topic: " + topicName + ": " + jobStatusQueryResponse);
      }
      assertEquals(
          executionStatus,
          ExecutionStatus.COMPLETED,
          "Push is yet to complete: " + jobStatusQueryResponse.toString());
    });
  }

  public static Store createTestStore(String name, String owner, long createdTime) {
    Store store = new ZKStore(
        name,
        owner,
        createdTime,
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        3); // TODO: figure out how to get hold of a sensible RF value here
    // Set the default timestamp to make sure every creation will return the same Store object.
    store.setLatestVersionPromoteToCurrentTimestamp(-1);
    return store;
  }

  /**
   * @deprecated
   * TODO: migrate to use ServiceFactory for generating a participant
   * */
  @Deprecated
  public static SafeHelixManager getParticipant(
      String cluster,
      String nodeId,
      String zkAddress,
      int httpPort,
      String stateModelDef) {
    ZkClient foo = new ZkClient(zkAddress);
    foo.close();
    VeniceOfflinePushMonitorAccessor offlinePushStatusAccessor =
        new VeniceOfflinePushMonitorAccessor(cluster, new ZkClient(zkAddress), new HelixAdapterSerializer(), 3, 1000);
    MockTestStateModelFactory stateModelFactory = new MockTestStateModelFactory(offlinePushStatusAccessor);
    return getParticipant(cluster, nodeId, zkAddress, httpPort, stateModelFactory, stateModelDef);
  }

  public static SafeHelixManager getParticipant(
      String cluster,
      String nodeId,
      String zkAddress,
      int httpPort,
      StateModelFactory<StateModel> stateModelFactory,
      String stateModelDef) {
    SafeHelixManager participant = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(cluster, nodeId, InstanceType.PARTICIPANT, zkAddress));
    participant.getStateMachineEngine().registerStateModelFactory(stateModelDef, stateModelFactory);
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

  public static VeniceControllerMultiClusterConfig getMultiClusterConfigFromOneCluster(
      VeniceControllerConfig controllerConfig) {
    Map<String, VeniceControllerConfig> configMap = new HashMap<>();
    configMap.put(controllerConfig.getClusterName(), controllerConfig);
    return new VeniceControllerMultiClusterConfig(configMap);
  }

  public static Properties getPropertiesForControllerConfig() throws IOException {
    String currentPath = Paths.get("").toAbsolutePath().toString();
    if (currentPath.endsWith("venice-controller")) {
      currentPath += "/..";
    } else if (currentPath.endsWith("venice-test-common")) {
      currentPath += "/../../services";
    }
    VeniceProperties clusterProps = Utils.parseProperties(currentPath + "/venice-server/config/cluster.properties");
    VeniceProperties baseControllerProps =
        Utils.parseProperties(currentPath + "/venice-controller/config/controller.properties");
    Properties properties = new Properties();
    properties.putAll(clusterProps.toProperties());
    properties.putAll(baseControllerProps.toProperties());
    return properties;
  }

  public static String getClusterToD2String(Map<String, String> clusterToD2) {
    return clusterToD2.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(","));
  }

  public static VeniceWriterFactory getVeniceWriterFactory(String kafkaBootstrapServers) {
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    return getVeniceWriterFactory(properties);
  }

  public static VeniceWriterFactory getVeniceWriterFactory(Properties properties) {
    Properties factoryProperties = new Properties();
    factoryProperties.putAll(properties);
    return new VeniceWriterFactory(factoryProperties);
  }

  public static SharedKafkaProducerAdapterFactory getSharedKafkaProducerService(Properties properties) {
    Properties factoryProperties = new Properties();
    factoryProperties.putAll(properties);
    return new SharedKafkaProducerAdapterFactory(
        factoryProperties,
        1,
        new ApacheKafkaProducerAdapterFactory(),
        new MetricsRepository(),
        new HashSet<>(
            Arrays.asList(
                "outgoing-byte-rate",
                "record-send-rate",
                "batch-size-max",
                "batch-size-avg",
                "buffer-available-bytes",
                "buffer-exhausted-rate")));
  }

  public static VeniceWriterFactory getVeniceWriterFactoryWithSharedProducer(
      Properties properties,
      SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory) {
    Properties factoryProperties = new Properties();
    factoryProperties.putAll(properties);
    return new VeniceWriterFactory(
        factoryProperties,
        sharedKafkaProducerAdapterFactory,
        sharedKafkaProducerAdapterFactory.getMetricsRepository());
  }

  public static Store getRandomStore() {
    return new ZKStore(
        getUniqueString("RandomStore"),
        getUniqueString("RandomOwner"),
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
      public void checkPermission(Permission perm) {
      }

      @Override
      public void checkPermission(Permission perm, Object context) {
      }

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

  public static void createAndVerifyStoreInAllRegions(
      String storeName,
      ControllerClient parentControllerClient,
      List<ControllerClient> controllerClientList) {
    Assert
        .assertFalse(parentControllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA).isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      for (ControllerClient client: controllerClientList) {
        Assert.assertFalse(client.getStore(storeName).isError());
      }
    });
  }

  public static void verifyDCConfigNativeAndActiveRepl(
      String storeName,
      boolean enabledNR,
      boolean enabledAA,
      ControllerClient... controllerClients) {
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      for (ControllerClient controllerClient: controllerClients) {
        StoreResponse storeResponse = assertCommand(controllerClient.getStore(storeName));
        Assert.assertEquals(
            storeResponse.getStore().isNativeReplicationEnabled(),
            enabledNR,
            "The native replication config does not match.");
        Assert.assertEquals(
            storeResponse.getStore().isActiveActiveReplicationEnabled(),
            enabledAA,
            "The active active replication config does not match.");
      }
    });
  }

  public static StoreIngestionTaskFactory.Builder getStoreIngestionTaskBuilder(String storeName) {
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(mockVeniceServerConfig).isHybridQuotaEnabled();
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();

    StorageEngineRepository mockStorageEngineRepository = mock(StorageEngineRepository.class);
    doReturn(mock(AbstractStorageEngine.class)).when(mockStorageEngineRepository).getLocalStorageEngine(anyString());

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    // Set timeout threshold to 0 so that push timeout error will happen immediately after a partition subscription.
    doReturn(0).when(mockStore).getBootstrapToOnlineTimeoutInHours();

    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(Collections.emptyMap()).when(mockOffsetRecord).getProducerPartitionStateMap();
    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    doReturn(mockOffsetRecord).when(mockStorageMetadataService).getLastOffset(eq(versionTopic), eq(0));

    int partitionCount = 1;
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(1);

    Version version = new VersionImpl(storeName, 1, "1", partitionCount);

    version.setPartitionerConfig(partitionerConfig);
    doReturn(partitionerConfig).when(mockStore).getPartitionerConfig();

    version.setIncrementalPushEnabled(false);
    doReturn(false).when(mockStore).isIncrementalPushEnabled();

    version.setHybridStoreConfig(null);
    doReturn(null).when(mockStore).getHybridStoreConfig();
    doReturn(false).when(mockStore).isHybrid();

    version.setBufferReplayEnabledForHybrid(true);

    version.setNativeReplicationEnabled(false);
    doReturn(false).when(mockStore).isNativeReplicationEnabled();

    version.setPushStreamSourceAddress("");
    doReturn("").when(mockStore).getPushStreamSourceAddress();

    doReturn(false).when(mockStore).isWriteComputationEnabled();

    doReturn(partitionCount).when(mockStore).getPartitionCount();

    doReturn(-1).when(mockStore).getCurrentVersion();

    doReturn(Optional.of(version)).when(mockStore).getVersion(anyInt());

    return new StoreIngestionTaskFactory.Builder().setVeniceWriterFactory(mock(VeniceWriterFactory.class))
        .setStorageEngineRepository(mockStorageEngineRepository)
        .setStorageMetadataService(mockStorageMetadataService)
        .setLeaderFollowerNotifiersQueue(new ArrayDeque<>())
        .setSchemaRepository(mock(ReadOnlySchemaRepository.class))
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setTopicManagerRepository(mock(TopicManagerRepository.class))
        .setHostLevelIngestionStats(mock(AggHostLevelIngestionStats.class))
        .setVersionedDIVStats(mock(AggVersionedDIVStats.class))
        .setVersionedIngestionStats(mock(AggVersionedIngestionStats.class))
        .setStoreBufferService(mock(StoreBufferService.class))
        .setDiskUsage(mock(DiskUsage.class))
        .setAggKafkaConsumerService(mock(AggKafkaConsumerService.class))
        .setServerConfig(mock(VeniceServerConfig.class))
        .setServerConfig(mockVeniceServerConfig)
        .setPartitionStateSerializer(mock(InternalAvroSpecificSerializer.class))
        .setIsDaVinciClient(false);
  }

  public static Map<byte[], byte[]> generateInput(
      int recordCnt,
      boolean sorted,
      int startId,
      AvroSerializer serializer) {
    Map<byte[], byte[]> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        ByteBuffer b1 = ByteBuffer.wrap(o1);
        ByteBuffer b2 = ByteBuffer.wrap(o2);
        return comparator.compare(b1, b2);
      });
    } else {
      records = new HashMap<>();
    }
    for (int i = startId; i < recordCnt + startId; ++i) {
      records.put(serializer.serialize("key" + i), serializer.serialize("value" + i));
    }
    return records;
  }

  public static void shutdownThread(Thread thread) throws InterruptedException {
    shutdownThread(thread, 5, TimeUnit.SECONDS);
  }

  public static void shutdownThread(Thread thread, long timeout, TimeUnit unit) throws InterruptedException {
    if (thread == null) {
      return;
    }
    thread.interrupt();
    thread.join(unit.toMillis(timeout));
    Assert.assertFalse(thread.isAlive());
  }

  public static void shutdownExecutor(ExecutorService executor) throws InterruptedException {
    shutdownExecutor(executor, 5, TimeUnit.SECONDS);
  }

  public static void shutdownExecutor(ExecutorService executor, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    executor.shutdownNow();
    Assert.assertTrue(executor.awaitTermination(timeout, unit));
  }

  public static void addIngestionIsolationToProperties(Properties properties) {
    properties.putAll(getIngestionIsolationPropertyMap());
  }

  public static Map<String, Object> getIngestionIsolationPropertyMap() {
    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(SERVER_INGESTION_MODE, IngestionMode.ISOLATED);
    propertyMap.put(SERVER_FORKED_PROCESS_JVM_ARGUMENT_LIST, "-Xms256M;-Xmx1G");
    return propertyMap;
  }

  public static String getUniqueTopicString(String prefix) {
    int typesNum = PubSubTopicType.values().length;
    int pubSubTopicTypeIndex = Math.abs(ThreadLocalRandom.current().nextInt() % typesNum);
    PubSubTopicType pubSubTopicType = PubSubTopicType.values()[pubSubTopicTypeIndex];
    int version = Math.abs(ThreadLocalRandom.current().nextInt() % typesNum);
    if (pubSubTopicType.equals(PubSubTopicType.REALTIME_TOPIC)) {
      return getUniqueString(prefix) + Version.REAL_TIME_TOPIC_SUFFIX;
    } else if (pubSubTopicType.equals(PubSubTopicType.REPROCESSING_TOPIC)) {
      return getUniqueString(prefix) + Version.VERSION_SEPARATOR + (version) + Version.STREAM_REPROCESSING_TOPIC_SUFFIX;
    } else if (pubSubTopicType.equals(PubSubTopicType.VERSION_TOPIC)) {
      return getUniqueString(prefix) + Version.VERSION_SEPARATOR + (version);
    } else if (pubSubTopicType.equals(PubSubTopicType.ADMIN_TOPIC)) {
      return pubSubTopicType.ADMIN_TOPIC_PREFIX + getUniqueString(prefix);
    } else if (pubSubTopicType.equals(PubSubTopicType.VIEW_TOPIC)) {
      return getUniqueString(prefix) + Version.VERSION_SEPARATOR + (version)
          + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    } else if (pubSubTopicType.equals(PubSubTopicType.UNKNOWN_TYPE_TOPIC)) {
      return getUniqueString(prefix);
    } else {
      throw new VeniceException("Unsupported topic type for: " + pubSubTopicType);
    }
  }

  /**
   * WARNING: The code which generates the free port and uses it must always be called within
   * a try/catch and a loop. There is no guarantee that the port returned will still be
   * available at the time it is used. This is best-effort only.
   *
   * @return a free port to be used by tests.
   */
  public static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
