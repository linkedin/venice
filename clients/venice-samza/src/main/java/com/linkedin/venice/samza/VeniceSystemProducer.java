package com.linkedin.venice.samza;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.pushmonitor.RouterBasedHybridStoreQuotaMonitor;
import com.linkedin.venice.pushmonitor.RouterBasedPushMonitor;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.BoundedHashMap;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;


/**
 * {@code VeniceSystemProducer} defines the interfaces for Samza jobs to send data to Venice stores.
 *
 * Samza jobs talk to either parent or child controller depending on the aggregate mode config.
 * The decision of which controller should be used is made in {@link VeniceSystemFactory}.
 * The "Primary Controller" term is used to refer to whichever controller the Samza job should talk to.
 *
 * The primary controller should be:
 * 1. The parent controller when the Venice system is deployed in a multi-colo mode and either:
 *     a. {@link Version.PushType} is {@link PushType.BATCH} or {@link PushType.STREAM_REPROCESSING}; or
 *     b. @deprecated {@link Version.PushType} is {@link PushType.STREAM} and the job is configured to write data in AGGREGATE mode
 * 2. The child controller when either:
 *     a. The Venice system is deployed in a single-colo mode; or
 *     b. The {@link Version.PushType} is {@link PushType.STREAM} and the job is configured to write data in NON_AGGREGATE mode
 */
public class VeniceSystemProducer implements SystemProducer, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemProducer.class);

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final Schema BOOL_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final DatumWriter<Utf8> STRING_DATUM_WRITER = new GenericDatumWriter<>(STRING_SCHEMA);
  private static final DatumWriter<Integer> INT_DATUM_WRITER = new GenericDatumWriter<>(INT_SCHEMA);
  private static final DatumWriter<Long> LONG_DATUM_WRITER = new GenericDatumWriter<>(LONG_SCHEMA);
  private static final DatumWriter<Float> FLOAT_DATUM_WRITER = new GenericDatumWriter<>(FLOAT_SCHEMA);
  private static final DatumWriter<Double> DOUBLE_DATUM_WRITER = new GenericDatumWriter<>(DOUBLE_SCHEMA);
  private static final DatumWriter<ByteBuffer> BYTES_DATUM_WRITER = new GenericDatumWriter<>(BYTES_SCHEMA);
  private static final DatumWriter<Boolean> BOOL_DATUM_WRITER = new GenericDatumWriter<>(BOOL_SCHEMA);

  private static final WriteComputeHandlerV1 writeComputeHandlerV1 = new WriteComputeHandlerV1();

  // Immutable state
  private final String veniceChildD2ZkHost;
  private final String primaryControllerColoD2ZKHost;
  private final String primaryControllerD2ServiceName;
  private final String storeName;
  private final String samzaJobId;
  private final Version.PushType pushType;
  private final Optional<SSLFactory> sslFactory;
  private final VeniceSystemFactory factory;
  private final Optional<String> partitioners;
  private final Time time;
  private final String runningFabric;
  private final boolean verifyLatestProtocolPresent;
  private final Map<String, D2ClientEnvelope> d2ZkHostToClientEnvelopeMap = new HashMap<>();
  private final VeniceConcurrentHashMap<Schema, Pair<Integer, Integer>> valueSchemaToIdsMap =
      new VeniceConcurrentHashMap<>();

  private final VeniceConcurrentHashMap<Pair<Integer, Integer>, Schema> valueSchemaIdsToSchemaMap =
      new VeniceConcurrentHashMap<>();

  // Mutable, lazily initialized, state
  private Schema keySchema;
  private String canonicalKeySchemaStr;
  // To avoid the excessive usage of the cache in case each message is using a unique key schema
  private final Map<Schema, String> canonicalSchemaStrCache = new BoundedHashMap<>(10, true);

  private D2Client primaryControllerColoD2Client;
  private D2Client childColoD2Client;
  private ControllerClient controllerClient;
  // It can be version topic, real-time topic or stream reprocessing topic, depending on push type
  private String topicName;
  private String kafkaBootstrapServers;

  private boolean isWriteComputeEnabled = false;
  private boolean isChunkingEnabled = false;

  private boolean isStarted = false;

  private Optional<String> discoveryUrl = Optional.empty();
  private Optional<String> routerUrl = Optional.empty();

  private VeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private Optional<RouterBasedPushMonitor> pushMonitor = Optional.empty();
  private Optional<RouterBasedHybridStoreQuotaMonitor> hybridStoreQuotaMonitor = Optional.empty();

  private Map<String, String> additionalWriterConfigs = new HashMap<>();

  @Deprecated
  public VeniceSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    this(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}. Equivalent to {@link VeniceSystemProducer(veniceChildD2ZkHost, primaryControllerColoD2ZKHost, primaryControllerD2ServiceName, storeName, pushType, samzaJobId, runningFabric, verifyLatestProtocolPresent, factory, sslFactory, partitioners, SystemTime.INSTANCE)}
   */
  public VeniceSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners) {
    this(
        veniceChildD2ZkHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners,
        SystemTime.INSTANCE);
  }

  @Deprecated
  public VeniceSystemProducer(
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    this(
        primaryControllerColoD2ZKHost,
        primaryControllerColoD2ZKHost,
        primaryControllerD2ServiceName,
        storeName,
        pushType,
        samzaJobId,
        runningFabric,
        verifyLatestProtocolPresent,
        factory,
        sslFactory,
        partitioners,
        time);
  }

  /**
   * Construct a new instance of {@link VeniceSystemProducer}
   * @param veniceChildD2ZkHost D2 Zk Address where the components in the child colo are announcing themselves
   * @param primaryControllerColoD2ZKHost D2 Zk Address of the colo where the primary controller resides
   * @param primaryControllerD2ServiceName The service name that the primary controller uses to announce itself to D2
   * @param storeName The store to write to
   * @param pushType The {@link Version.PushType} to use to write to the store
   * @param samzaJobId A unique id used to identify jobs that can concurrently write to the same store
   * @param runningFabric The colo where the job is running. It is used to find the best destination for the data to be written to
   * @param verifyLatestProtocolPresent Config to check whether the protocol versions used at runtime are valid in Venice backend
   * @param factory The {@link VeniceSystemFactory} object that was used to create this object
   * @param sslFactory An optional {@link SSLFactory} that is used to communicate with other components using SSL
   * @param partitioners A list of comma-separated partitioners class names that are supported.
   * @param time An object of type {@link Time}. It is helpful to be configurable for testing.
   */
  public VeniceSystemProducer(
      String veniceChildD2ZkHost,
      String primaryControllerColoD2ZKHost,
      String primaryControllerD2ServiceName,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {
    this.veniceChildD2ZkHost = veniceChildD2ZkHost;
    this.primaryControllerColoD2ZKHost = primaryControllerColoD2ZKHost;
    this.primaryControllerD2ServiceName = primaryControllerD2ServiceName;
    this.storeName = storeName;
    this.pushType = pushType;
    this.samzaJobId = samzaJobId;
    this.runningFabric = runningFabric;
    this.verifyLatestProtocolPresent = verifyLatestProtocolPresent;
    this.factory = factory;
    this.sslFactory = sslFactory;
    this.partitioners = partitioners;
    this.time = time;
  }

  public VeniceSystemProducer(
      String discoveryUrl,
      String storeName,
      Version.PushType pushType,
      String samzaJobId,
      String runningFabric,
      boolean verifyLatestProtocolPresent,
      VeniceSystemFactory factory,
      Optional<SSLFactory> sslFactory,
      Optional<String> partitioners,
      Time time) {

    if (discoveryUrl == null || discoveryUrl.trim().isEmpty()) {
      throw new IllegalStateException("Discovery URL is not present");
    }

    this.discoveryUrl = Optional.of(discoveryUrl);

    this.veniceChildD2ZkHost = null;
    this.primaryControllerColoD2ZKHost = null;
    this.primaryControllerD2ServiceName = null;

    this.storeName = storeName;
    this.pushType = pushType;
    this.samzaJobId = samzaJobId;
    this.runningFabric = runningFabric;
    this.verifyLatestProtocolPresent = verifyLatestProtocolPresent;
    this.factory = factory;
    this.sslFactory = sslFactory;
    this.partitioners = partitioners;
    this.time = time;
  }

  public void applyAdditionalWriterConfigs(Map<String, String> additionalWriterConfigs) {
    this.additionalWriterConfigs.putAll(additionalWriterConfigs);
  }

  public void setRouterUrl(String routerUrl) {
    this.routerUrl = Optional.of(routerUrl);
  }

  public String getRunningFabric() {
    return this.runningFabric;
  }

  protected ControllerResponse controllerRequestWithRetry(Supplier<ControllerResponse> supplier, int retryLimit) {
    String errorMsg = "";
    Exception lastException = null;
    for (int currentAttempt = 0; currentAttempt < retryLimit; currentAttempt++) {
      lastException = null;
      try {
        ControllerResponse controllerResponse = supplier.get();
        if (!controllerResponse.isError()) {
          return controllerResponse;
        } else {
          time.sleep(1000L * (currentAttempt + 1));
          errorMsg = controllerResponse.getError();
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          throw new VeniceException(e);
        }
        try {
          time.sleep(1000L * (currentAttempt + 1));
        } catch (InterruptedException ie) {
          throw new VeniceException(ie);
        }
        errorMsg = e.getMessage();
        lastException = e;
      }
    }
    throw new SamzaException("Failed to send request to Controller, error: " + errorMsg, lastException);
  }

  public String getTopicName() {
    return topicName;
  }

  /**
   * This method is overridden and not used by LinkedIn internally.
   * Please update the overridden method accordingly after modifying this method.
   */
  protected VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(VersionCreationResponse store) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, store.getKafkaBootstrapServers());
    return getVeniceWriter(store, veniceWriterProperties);
  }

  /**
   * Please don't remove this method since it is still being used by LinkedIn internally.
   */
  protected VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(
      VersionCreationResponse store,
      Properties veniceWriterProperties) {
    int amplificationFactor = store.getAmplificationFactor();
    Integer partitionCount = pushType.isBatchOrStreamReprocessing()
        ? (store.getPartitions() * amplificationFactor)
        /**
         * N.B. There is an issue in the controller where the partition count inside a {@link VersionCreationResponse}
         *      for a non-batch topic is invalid, so in that case we don't rely on it, and let the {@link VeniceWriter}
         *      figure it out by doing a metadata call to Kafka. It would be great to fix the controller bug and then
         *      always pass in the partition count here, so we can skip this MD call.
         */
        : null;
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(store.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        store.getPartitionerClass(),
        amplificationFactor,
        new VeniceProperties(partitionerProperties));
    return constructVeniceWriter(
        veniceWriterProperties,
        new VeniceWriterOptions.Builder(store.getKafkaTopic()).setTime(time)
            .setPartitioner(venicePartitioner)
            .setPartitionCount(partitionCount)
            .setChunkingEnabled(isChunkingEnabled)
            .build());
  }

  // trickery for unit testing
  VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(Properties properties, VeniceWriterOptions writerOptions) {
    Properties finalWriterConfigs = new Properties();
    finalWriterConfigs.putAll(properties);
    finalWriterConfigs.putAll(additionalWriterConfigs);
    return new VeniceWriterFactory(finalWriterConfigs, new ApacheKafkaProducerAdapterFactory(), null)
        .createVeniceWriter(writerOptions);
  }

  @Override
  public synchronized void start() {
    if (this.isStarted) {
      return;
    }
    this.isStarted = true;

    final TransportClient transportClient;
    if (discoveryUrl.isPresent()) {
      this.controllerClient =
          ControllerClientFactory.discoverAndConstructControllerClient(storeName, discoveryUrl.get(), sslFactory, 1);

      /**
       * Verify that the latest {@link com.linkedin.venice.serialization.avro.AvroProtocolDefinition#KAFKA_MESSAGE_ENVELOPE}
       * version in the code base is registered in Venice backend; if not, fail fast in start phase before start writing
       * Kafka messages that Venice backend couldn't deserialize.
       */
      if (verifyLatestProtocolPresent && routerUrl.isPresent()) {
        LOGGER.info("Start verifying the latest protocols at runtime are valid in Venice backend.");
        String kafkaMessageEnvelopSchemaSysStore = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();
        ClientConfig clientConfigForKafkaMessageEnvelopeSchemaReader =
            ClientConfig.defaultGenericClientConfig(kafkaMessageEnvelopSchemaSysStore);
        clientConfigForKafkaMessageEnvelopeSchemaReader.setVeniceURL(routerUrl.get());

        SchemaReader kafkaMessageEnvelopeSchemaReader =
            ClientFactory.getSchemaReader(clientConfigForKafkaMessageEnvelopeSchemaReader, null);
        new SchemaPresenceChecker(kafkaMessageEnvelopeSchemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)
            .verifySchemaVersionPresentOrExit();
        LOGGER.info("Successfully verified the latest protocols at runtime are valid in Venice backend.");
      } else {
        LOGGER.info("Skip verifying the latest protocols at runtime are valid in Venice backend.");
      }

      if (sslFactory.isPresent()) {
        transportClient = new HttpsTransportClient(discoveryUrl.get(), 0, 0, false, sslFactory.get());
      } else {
        transportClient = new HttpTransportClient(discoveryUrl.get(), 0, 0);
      }
    } else {
      this.primaryControllerColoD2Client = getStartedD2Client(primaryControllerColoD2ZKHost);
      this.childColoD2Client = getStartedD2Client(veniceChildD2ZkHost);

      // Discover cluster
      D2ServiceDiscoveryResponse discoveryResponse = (D2ServiceDiscoveryResponse) controllerRequestWithRetry(
          () -> D2ControllerClient
              .discoverCluster(primaryControllerColoD2Client, primaryControllerD2ServiceName, this.storeName),
          10);
      String clusterName = discoveryResponse.getCluster();
      LOGGER.info("Found cluster: {} for store: {}", clusterName, storeName);

      /**
       * Verify that the latest {@link com.linkedin.venice.serialization.avro.AvroProtocolDefinition#KAFKA_MESSAGE_ENVELOPE}
       * version in the code base is registered in Venice backend; if not, fail fast in start phase before start writing
       * Kafka messages that Venice backend couldn't deserialize.
       */
      if (verifyLatestProtocolPresent) {
        LOGGER.info("Start verifying the latest protocols at runtime are valid in Venice backend.");
        // Discover the D2 service name for the system store
        String kafkaMessageEnvelopSchemaSysStore = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();
        D2ServiceDiscoveryResponse sysStoreDiscoveryResponse = (D2ServiceDiscoveryResponse) controllerRequestWithRetry(
            () -> D2ControllerClient.discoverCluster(
                primaryControllerColoD2Client,
                primaryControllerD2ServiceName,
                kafkaMessageEnvelopSchemaSysStore),
            2);
        ClientConfig clientConfigForKafkaMessageEnvelopeSchemaReader =
            ClientConfig.defaultGenericClientConfig(kafkaMessageEnvelopSchemaSysStore);
        clientConfigForKafkaMessageEnvelopeSchemaReader.setD2ServiceName(sysStoreDiscoveryResponse.getD2Service());
        clientConfigForKafkaMessageEnvelopeSchemaReader.setD2Client(childColoD2Client);
        SchemaReader kafkaMessageEnvelopeSchemaReader =
            ClientFactory.getSchemaReader(clientConfigForKafkaMessageEnvelopeSchemaReader, null);
        new SchemaPresenceChecker(kafkaMessageEnvelopeSchemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)
            .verifySchemaVersionPresentOrExit();
        LOGGER.info("Successfully verified the latest protocols at runtime are valid in Venice backend.");
      }

      this.controllerClient = new D2ControllerClient(
          primaryControllerD2ServiceName,
          clusterName,
          primaryControllerColoD2Client,
          sslFactory);
      transportClient = new D2TransportClient(discoveryResponse.getD2Service(), childColoD2Client);
    }

    // Request all the necessary info from Venice Controller
    VersionCreationResponse versionCreationResponse = (VersionCreationResponse) controllerRequestWithRetry(
        () -> this.controllerClient.requestTopicForWrites(
            this.storeName,
            1,
            pushType,
            samzaJobId,
            true, // sendStartOfPush must be true in order to support batch push to Venice from Samza app
            false, // Samza jobs, including batch ones, are expected to write data out of order
            false,
            partitioners,
            Optional.empty(),
            Optional.ofNullable(runningFabric),
            false,
            -1),
        2);
    LOGGER.info("Got [store: {}] VersionCreationResponse: {}", storeName, versionCreationResponse);
    this.topicName = versionCreationResponse.getKafkaTopic();
    this.kafkaBootstrapServers = versionCreationResponse.getKafkaBootstrapServers();

    StoreResponse storeResponse =
        (StoreResponse) controllerRequestWithRetry(() -> this.controllerClient.getStore(storeName), 2);
    this.isWriteComputeEnabled = storeResponse.getStore().isWriteComputationEnabled();

    boolean hybridStoreDiskQuotaEnabled = storeResponse.getStore().isHybridStoreDiskQuotaEnabled();

    SchemaResponse keySchemaResponse =
        (SchemaResponse) controllerRequestWithRetry(() -> this.controllerClient.getKeySchema(this.storeName), 2);
    LOGGER.info("Got [store: {}] SchemaResponse for key schema: {}", storeName, keySchemaResponse);
    this.keySchema = parseSchemaFromJSONStrictValidation(keySchemaResponse.getSchemaStr());
    this.canonicalKeySchemaStr = AvroCompatibilityHelper.toParsingForm(this.keySchema);

    // Load Schemas from Venice
    refreshSchemaCache();

    if (pushType.equals(Version.PushType.STREAM_REPROCESSING)) {
      String versionTopic = Version.composeVersionTopicFromStreamReprocessingTopic(topicName);
      pushMonitor = Optional.of(new RouterBasedPushMonitor(transportClient, versionTopic, factory, this));
      pushMonitor.get().start();
    }

    if (pushType.isBatchOrStreamReprocessing()) {
      int versionNumber = versionCreationResponse.getVersion();
      Version version = storeResponse.getStore()
          .getVersion(versionNumber)
          .orElseThrow(
              () -> new VeniceException(
                  "Version info for version " + versionNumber + " not available in store response"));
      // For pushes made to VT or SR topic, the producer should chunk the data
      this.isChunkingEnabled = version.isChunkingEnabled();
    } else {
      // For pushes made to RT, the producer should not chunk the data
      this.isChunkingEnabled = false;
    }

    this.veniceWriter = getVeniceWriter(versionCreationResponse);

    if (pushMonitor.isPresent()) {
      /**
       * If the stream reprocessing job has finished, push monitor will exit the Samza process directly.
       */
      ExecutionStatus currentStatus = pushMonitor.get().getCurrentStatus();
      if (ExecutionStatus.ERROR.equals(currentStatus)) {
        throw new VeniceException(
            "Push job for resource " + topicName + " is in error state; please reach out to Venice team.");
      }
    }

    if ((pushType.equals(Version.PushType.STREAM) || pushType.equals(Version.PushType.STREAM_REPROCESSING))
        && hybridStoreDiskQuotaEnabled) {
      hybridStoreQuotaMonitor =
          Optional.of(new RouterBasedHybridStoreQuotaMonitor(transportClient, storeName, pushType, topicName));
      hybridStoreQuotaMonitor.get().start();
    }
  }

  // Grabs all Venice schemas and their associated ID's and caches them
  private void refreshSchemaCache() {
    MultiSchemaResponse valueSchemaResponse = (MultiSchemaResponse) controllerRequestWithRetry(
        () -> this.controllerClient.getAllValueAndDerivedSchema(this.storeName),
        2);
    LOGGER.info("Got [store: {}] SchemaResponse for value schemas: {}", storeName, valueSchemaResponse);
    for (MultiSchemaResponse.Schema valueSchema: valueSchemaResponse.getSchemas()) {
      Schema schema = parseSchemaFromJSONLooseValidation(valueSchema.getSchemaStr());
      Pair<Integer, Integer> idPair = new Pair<>(valueSchema.getId(), valueSchema.getDerivedSchemaId());
      valueSchemaToIdsMap.put(schema, idPair);
      valueSchemaIdsToSchemaMap.put(idPair, schema);
    }
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public synchronized void stop() {
    this.isStarted = false;
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    if (Version.PushType.STREAM_REPROCESSING.equals(pushType) && pushMonitor.isPresent()) {
      String versionTopic = Version.composeVersionTopicFromStreamReprocessingTopic(topicName);
      switch (pushMonitor.get().getCurrentStatus()) {
        case COMPLETED:
          LOGGER.info("Push job for {} is COMPLETED.", topicName);
          break;
        case END_OF_PUSH_RECEIVED:
          LOGGER.info("Batch load for {} has finished.", topicName);
          break;
        case ERROR:
          LOGGER.info("Push job for {} encountered error.", topicName);
          break;
        default:
          LOGGER.warn("Push job in Venice backend is still in progress... Will clean up resources in Venice");
          /**
           * Consider there could be hundreds of Samza containers for stream reprocessing job, we shouldn't let all
           * the containers send kill requests to controller at the same time to avoid hammering on controller.
           */
          Utils.sleep(ThreadLocalRandom.current().nextInt(30000));
          controllerClient.retryableRequest(3, c -> c.killOfflinePushJob(versionTopic));
          LOGGER.info("Offline push job has been killed, topic: {}", versionTopic);
      }
      Utils.closeQuietlyWithErrorLogged(pushMonitor.get());
    }
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    hybridStoreQuotaMonitor.ifPresent(Utils::closeQuietlyWithErrorLogged);
    d2ZkHostToClientEnvelopeMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
  }

  @Override
  public void register(String source) {

  }

  @Override
  public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
    if (!isStarted) {
      throw new SamzaException("Send called on Venice System Producer that is not started yet!");
    }
    String storeOfIncomingMessage = outgoingMessageEnvelope.getSystemStream().getStream();
    if (!storeOfIncomingMessage.equals(storeName)) {
      throw new SamzaException(
          "The store of the incoming message: " + storeOfIncomingMessage + " is unexpected, and it should be "
              + storeName);
    }

    if (pushMonitor.isPresent() && Version.PushType.STREAM_REPROCESSING.equals(pushType)) {
      ExecutionStatus currentStatus = pushMonitor.get().getCurrentStatus();
      switch (currentStatus) {
        case ERROR:
          /**
           * If there are multiple stream reprocessing SystemProducer in one Samza job, one failed push will
           * also affect other push jobs.
           */
          throw new VeniceException(
              "Push job for resource " + topicName + " is in error state; please reach out to Venice team.");
        case END_OF_PUSH_RECEIVED:
        case COMPLETED:
          LOGGER.info("Stream reprocessing for resource {} has finished. No message will be sent.", topicName);
          return;
        default:
          // no-op
      }
    }
    if (hybridStoreQuotaMonitor.isPresent()
        && (Version.PushType.STREAM.equals(pushType) || Version.PushType.STREAM_REPROCESSING.equals(pushType))) {
      HybridStoreQuotaStatus currentStatus = hybridStoreQuotaMonitor.get().getCurrentStatus();
      switch (currentStatus) {
        case QUOTA_VIOLATED:
          /**
           * If there are multiple stream SystemProducer in one Samza job, one failed push will
           * also affect other push jobs.
           */
          LOGGER.error("Current hybrid store quota status: {}, should throw exception to kill the job.", currentStatus);
          throw new VeniceException(
              "Push job for resource " + topicName
                  + " is in hybrid quota violated mode; please reach out to Venice team.");
        case QUOTA_NOT_VIOLATED:
        default:
          // no-op
      }
    }

    send(outgoingMessageEnvelope.getKey(), outgoingMessageEnvelope.getMessage());
  }

  protected CompletableFuture<Void> send(Object keyObject, Object valueObject) {
    Schema keyObjectSchema = getSchemaFromObject(keyObject);
    String canonicalSchemaStr = canonicalSchemaStrCache
        .computeIfAbsent(keyObjectSchema, k -> AvroCompatibilityHelper.toParsingForm(keyObjectSchema));

    if (!canonicalKeySchemaStr.equals(canonicalSchemaStr)) {
      throw new SamzaException(
          "Cannot write record to Venice store " + storeName + ", key object has schema " + canonicalSchemaStr
              + " which does not match Venice key schema " + canonicalKeySchemaStr + ".");
    }

    byte[] key = serializeObject(keyObject);
    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    final PubSubProducerCallback callback = new CompletableFutureCallback(completableFuture);

    long logicalTimestamp = -1;
    // Only transmit the timestamp if this is a realtime topic.
    if (valueObject instanceof VeniceObjectWithTimestamp && Version.isRealTimeTopic(topicName)) {
      VeniceObjectWithTimestamp objectWithTimestamp = (VeniceObjectWithTimestamp) valueObject;
      logicalTimestamp = objectWithTimestamp.getTimestamp();
      if (logicalTimestamp <= 0) {
        throw new SamzaException(
            "Timestamp specified in passed `VeniceObjectWithTimestamp` object should be positive, but received: "
                + logicalTimestamp);
      }
      valueObject = objectWithTimestamp.getObject();
    }

    if (valueObject == null) {
      if (logicalTimestamp > 0) {
        veniceWriter.delete(key, logicalTimestamp, callback);
      } else {
        veniceWriter.delete(key, callback);
      }
    } else {
      Schema valueObjectSchema = getSchemaFromObject(valueObject);

      Pair<Integer, Integer> valueSchemaIdPair = valueSchemaToIdsMap.computeIfAbsent(valueObjectSchema, valueSchema -> {
        SchemaResponse valueSchemaResponse = (SchemaResponse) controllerRequestWithRetry(
            () -> controllerClient.getValueOrDerivedSchemaId(storeName, valueSchema.toString()),
            2);
        LOGGER.info("Got [store: {}] SchemaResponse for schema: {}", storeName, valueSchema);
        return new Pair<>(valueSchemaResponse.getId(), valueSchemaResponse.getDerivedSchemaId());
      });

      if (Version.isATopicThatIsVersioned(topicName) && valueSchemaIdPair.getSecond() != -1) {
        // This is a write compute request getting published to a version topic or reprocessing topic. We don't
        // support partial records in the Venice version topic, so we will convert this request
        // to a full put with default fields applied

        int baseSchemaId = valueSchemaIdPair.getFirst();
        valueObject = convertPartialUpdateToFullPut(valueSchemaIdPair, valueObject);
        valueSchemaIdPair = new Pair<>(baseSchemaId, -1);
      }

      byte[] value = serializeObject(valueObject);

      if (valueSchemaIdPair.getSecond() == -1) {
        if (logicalTimestamp > 0) {
          veniceWriter.put(key, value, valueSchemaIdPair.getFirst(), logicalTimestamp, callback);
        } else {
          veniceWriter.put(key, value, valueSchemaIdPair.getFirst(), callback);
        }
      } else {
        if (!isWriteComputeEnabled) {
          throw new SamzaException(
              "Cannot write partial update record to Venice store " + storeName + " "
                  + "because write-compute is not enabled for it. Please contact Venice team to configure it.");
        }
        if (logicalTimestamp > 0) {
          veniceWriter.update(
              key,
              value,
              valueSchemaIdPair.getFirst(),
              valueSchemaIdPair.getSecond(),
              callback,
              logicalTimestamp);
        } else {
          veniceWriter.update(key, value, valueSchemaIdPair.getFirst(), valueSchemaIdPair.getSecond(), callback);
        }
      }
    }
    return completableFuture;
  }

  public CompletableFuture<Void> put(Object keyObject, Object valueObject) {
    return send(keyObject, valueObject);
  }

  public CompletableFuture<Void> delete(Object keyObject) {
    return send(keyObject, null);
  }

  /**
   * Flushing the data to Venice store in case VeniceSystemProducer buffers message.
   *
   * @param s String representing the source of the message. Currently, VeniceSystemProducer is not using this param.
   */
  @Override
  public void flush(String s) {
    veniceWriter.flush();
  }

  private static Schema getSchemaFromObject(Object object) {
    if (object instanceof IndexedRecord) {
      IndexedRecord keyAvro = (IndexedRecord) object;
      return keyAvro.getSchema();
    } else if (object instanceof CharSequence) { // convenience option.
      return STRING_SCHEMA;
    } else if (object instanceof Integer) {
      return INT_SCHEMA;
    } else if (object instanceof Long) {
      return LONG_SCHEMA;
    } else if (object instanceof Double) {
      return DOUBLE_SCHEMA;
    } else if (object instanceof Float) {
      return FLOAT_SCHEMA;
    } else if (object instanceof byte[] || object instanceof ByteBuffer) {
      return BYTES_SCHEMA;
    } else if (object instanceof Boolean) {
      return BOOL_SCHEMA;
    } else {
      throw new SamzaException(
          "Venice System Producer only supports Avro objects, and primitives, found object of class: "
              + object.getClass().toString());
    }
  }

  private byte[] serializeObject(Object input) {
    if (input instanceof IndexedRecord) {
      RecordSerializer<Object> fastAvroSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(((IndexedRecord) input).getSchema());
      return fastAvroSerializer.serialize(input);
    } else if (input instanceof CharSequence) {
      return serializePrimitive(new Utf8(input.toString()), STRING_DATUM_WRITER);
    } else if (input instanceof Integer) {
      return serializePrimitive((Integer) input, INT_DATUM_WRITER);
    } else if (input instanceof Long) {
      return serializePrimitive((Long) input, LONG_DATUM_WRITER);
    } else if (input instanceof Double) {
      return serializePrimitive((Double) input, DOUBLE_DATUM_WRITER);
    } else if (input instanceof Float) {
      return serializePrimitive((Float) input, FLOAT_DATUM_WRITER);
    } else if (input instanceof ByteBuffer) {
      return serializePrimitive((ByteBuffer) input, BYTES_DATUM_WRITER);
    } else if (input instanceof byte[]) {
      return serializePrimitive(ByteBuffer.wrap((byte[]) input), BYTES_DATUM_WRITER);
    } else if (input instanceof Boolean) {
      return serializePrimitive((Boolean) input, BOOL_DATUM_WRITER);
    } else {
      throw new SamzaException(
          "Can only serialize avro objects, and primitives, cannot serialize: " + input.getClass().toString());
    }
  }

  /**
   * @param input primitive object to be serialized (Utf8, int, ...)
   * @param writer DatumWriter to use for the serialization
   * @param <T> type of the input
   * @return avro binary serialized byte[]
   */
  private static <T> byte[] serializePrimitive(T input, DatumWriter<T> writer) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(out);
    try {
      writer.write(input, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write input: " + input + " to binary encoder", e);
    }
    return out.toByteArray();
  }

  protected Object convertPartialUpdateToFullPut(Pair<Integer, Integer> schemaIds, Object incomingWriteValueObject) {
    Pair<Integer, Integer> baseSchemaIds = new Pair(schemaIds.getFirst(), -1);
    Schema baseSchema = valueSchemaIdsToSchemaMap.get(baseSchemaIds);
    if (baseSchema == null) {
      // refresh from venice once since we don't have this schema cached yet, then check again
      this.refreshSchemaCache();
      baseSchema = valueSchemaIdsToSchemaMap.get(baseSchemaIds);
      if (baseSchema == null) {
        // Something isn't right with this write. We can't seem to find an associated schema, so raise an exception.
        throw new SamzaException(
            "Unable to find base schema with id: " + schemaIds.getFirst() + " for write compute schema with id "
                + schemaIds.getSecond());
      }
    }
    return writeComputeHandlerV1.updateValueRecord(baseSchema, null, (GenericRecord) incomingWriteValueObject);
  }

  public void setExitMode(SamzaExitMode exitMode) {
    if (pushMonitor.isPresent()) {
      pushMonitor.get().setStreamReprocessingExitMode(exitMode);
    }
  }

  /**
   * Test methods
   */
  public String getKafkaBootstrapServers() {
    return this.kafkaBootstrapServers;
  }

  public VeniceWriter<byte[], byte[], byte[]> getInternalProducer() {
    return this.veniceWriter;
  }

  protected void setControllerClient(D2ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  private D2Client getStartedD2Client(String d2ZkHost) {
    D2ClientEnvelope d2ClientEnvelope = d2ZkHostToClientEnvelopeMap.computeIfAbsent(d2ZkHost, zkHost -> {
      String fsBasePath = Utils.getUniqueTempPath("d2");
      D2Client d2Client = new D2ClientBuilder().setZkHosts(d2ZkHost)
          .setSSLContext(sslFactory.map(SSLFactory::getSSLContext).orElse(null))
          .setIsSSLEnabled(sslFactory.isPresent())
          .setSSLParameters(sslFactory.map(SSLFactory::getSSLParameters).orElse(null))
          .setFsBasePath(fsBasePath)
          .setEnableSaveUriDataOnDisk(true)
          .build();
      D2ClientUtils.startClient(d2Client);
      return new D2ClientEnvelope(d2Client, fsBasePath);
    });
    return d2ClientEnvelope.d2Client;
  }

  private static final class D2ClientEnvelope implements Closeable {
    D2Client d2Client;
    String fsBasePath;

    D2ClientEnvelope(D2Client d2Client, String fsBasePath) {
      this.d2Client = d2Client;
      this.fsBasePath = fsBasePath;
    }

    @Override
    public void close() throws IOException {
      D2ClientUtils.shutdownClient(d2Client);
      try {
        FileUtils.deleteDirectory(new File(fsBasePath));
      } catch (IOException e) {
        LOGGER.info("Error in cleaning up: {}", fsBasePath);
      }
    }
  }
}
