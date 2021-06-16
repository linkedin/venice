package com.linkedin.venice.replication;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.offsets.OffsetRecord.*;


public class BrooklinTopicReplicator extends TopicReplicator {
  public static final String BROOKLIN_CONFIG_PREFIX = TOPIC_REPLICATOR_CONFIG_PREFIX + "brooklin.";
  public static final String BROOKLIN_CONNECTION_STRING = BROOKLIN_CONFIG_PREFIX + "connection.string";
  public static final String BROOKLIN_CONNECTION_APPLICATION_ID = BROOKLIN_CONFIG_PREFIX + "application.id";
  public static final String BROOKLIN_AUTO_OFFSET_RESET = "system.auto.offset.reset";

  private final DatastreamRestClient client;
  private final String veniceCluster;
  private final String applicationId;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final int MAX_DATASTREAM_CREATION_ATTEMPTS = 5;

  public static final String TRANSPORT_PROVIDER_NAME = "rawkafkakac";
  public static final String BROOKLIN_CONNECTOR_NAME = "RawKafka";

  public static final String KAFKA_PPREFIX = "kafka://";
  public static final String SSL_KAFKA_PREFIX = "kafkassl://";

  private static final Logger logger = Logger.getLogger(TopicManager.class);

  private boolean isSSLToKafka;

  /**
   * Main constructor. Used by reflection.
   *
   * @param topicManager
   * @param veniceProperties
   */
  public BrooklinTopicReplicator(TopicManager topicManager, VeniceWriterFactory veniceWriterFactory, VeniceProperties veniceProperties) {
    this(topicManager, veniceWriterFactory, veniceProperties.getString(BROOKLIN_CONNECTION_STRING),
        veniceProperties,
        veniceProperties.getString(ConfigKeys.CLUSTER_NAME),
        veniceProperties.getString(BROOKLIN_CONNECTION_APPLICATION_ID),
        veniceProperties.getBoolean(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, false),
        veniceProperties.getBoolean(ConfigKeys.BROOKLIN_SSL_ENABLED, false)
            ? Optional.of(new SSLConfig(veniceProperties))
            : Optional.empty()
    );
  }

  /**
   * Strongly-typed constructor. Used in unit tests...
   *
   * @param brooklinConnectionString For connecting to the brooklin cluster, http://host:port or d2://service
   * @param veniceProperties with the corresponding configs for the replicator.
   * @param topicManager TopicManager for checking information about the Kafka topics.
   * @param veniceCluster Name of the venice cluster, used to create unique datastream names
   * @param applicationId The name of the service using the BrooklinTopicReplicator, this gets used as the "owner" for any created datastreams
   * @param sslConfig Configs required for SSL connection to Brooklin
   */
  public BrooklinTopicReplicator(
      TopicManager topicManager,
      VeniceWriterFactory veniceWriterFactory,
      String brooklinConnectionString,
      VeniceProperties veniceProperties,
      String veniceCluster,
      String applicationId,
      boolean isKafkaSSL,
      Optional<SSLConfig> sslConfig) {
    super(topicManager, veniceWriterFactory, veniceProperties);

    this.veniceCluster = veniceCluster;
    this.applicationId = applicationId;
    this.isSSLToKafka = isKafkaSSL;

    Map<String, Object> httpConfig = new HashMap<>();
    httpConfig.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, String.valueOf(TimeUnit.SECONDS.toMillis(10)));

    if (sslConfig.isPresent()) {
      SSLContext sslContext;
      SSLParameters sslParameters;
      try {
        SSLEngineComponentFactoryImpl sslEngine = new SSLEngineComponentFactoryImpl(sslConfig.get().getSslEngineComponentConfig());
        sslContext = sslEngine.getSSLContext();
        sslParameters = sslEngine.getSSLParameters();
      } catch (Exception e) {
        logger.error("Failed to create ssl context and parameters", e);
        throw new VeniceException(e);
      }

      String canonicalUri = RestliUtils.sanitizeUri(brooklinConnectionString);
      if (!canonicalUri.startsWith("https://")) {
        throw new VeniceException("Expect URL starts with \"https://\", actual = " + canonicalUri);
      }

      httpConfig.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslContext);
      httpConfig.put(HttpClientFactory.HTTP_SSL_PARAMS, sslParameters);
      RestClient restClient = new RestClient(new TransportClientAdapter(new HttpClientFactory().getClient(httpConfig)), canonicalUri);
      DatastreamRestClientFactory.registerRestClient(canonicalUri, restClient);

      this.client = DatastreamRestClientFactory.getClient(canonicalUri);
    } else {
      this.client = DatastreamRestClientFactory.getClient(brooklinConnectionString, httpConfig);
    }
  }

  @Override
  public void prepareAndStartReplication(String srcTopicName, String destTopicName, Store store,
      String aggregateRealTimeSourceKafkaUrl) {
    Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(destTopicName));
    if (!version.isPresent()) {
      throw new VeniceException("Corresponding version does not exist for topic: " + destTopicName + " in store: "
          + store.getName());
    }
    Optional<HybridStoreConfig> hybridStoreConfig;
    if (version.get().isUseVersionLevelHybridConfig()) {
      hybridStoreConfig = Optional.ofNullable(version.get().getHybridStoreConfig());
    } else {
      hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());
    }
    checkPreconditions(srcTopicName, destTopicName, store, hybridStoreConfig);
    long bufferReplayStartTime = getRewindStartTime(hybridStoreConfig, version.get().getCreatedTime());
    beginReplication(srcTopicName, destTopicName, bufferReplayStartTime, null);
  }

  @Override
  void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount,
      long rewindStartTimestamp, String remoteKafkaUrl) {

    Map<Integer, Long> startingOffsetsMap = getTopicManager().getOffsetsByTime(sourceTopic, rewindStartTimestamp);
    logger.info("Get rewinding offset for topic: "+ sourceTopic + ", and rewind start timestamp: " + rewindStartTimestamp + ": " + startingOffsetsMap);

    List<Long> startingOffsets = startingOffsetsMap.entrySet().stream()
        .sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
        .map(entry -> entry.getValue())
        .collect(Collectors.toList());
    getVeniceWriterFactory().useVeniceWriter(
        () -> getVeniceWriterFactory().createBasicVeniceWriter(destinationTopic, getTimer()),
        // Online/Offline always consumes locally. Parameter remoteKafkaUrl is ignored.
        veniceWriter -> veniceWriter.broadcastStartOfBufferReplay(startingOffsets, destKafkaBootstrapServers, sourceTopic, new HashMap<>())
    );

    String name = datastreamName(sourceTopic, destinationTopic);

    Datastream datastream = new Datastream();
    datastream.setName(name);
    datastream.setConnectorName(BROOKLIN_CONNECTOR_NAME);

    StringMap metadata = new StringMap();

    metadata.put(DatastreamMetadataConstants.OWNER_KEY, applicationId); // application Id
    metadata.put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
    metadata.put(BROOKLIN_AUTO_OFFSET_RESET, "none");

    Map<String, Long> startingOffsetsStringMap = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : startingOffsetsMap.entrySet()){
      startingOffsetsStringMap.put(Integer.toString(entry.getKey()), (entry.getValue() == LOWEST_OFFSET) ? 0L : entry.getValue());
    }
    try {
      String startingOffsetsJson = mapper.writeValueAsString(startingOffsetsStringMap);
      metadata.put(DatastreamMetadataConstants.START_POSITION, startingOffsetsJson);
    } catch (IOException e) { // This shouldn't happen
      throw new VeniceException("There was a failure parsing starting offsets map to json: " + startingOffsetsStringMap.toString(), e);
    }
    datastream.setMetadata(metadata);

    DatastreamSource source  = new DatastreamSource();
    String sourceConnectionString = getKafkaURL(sourceTopic); // TODO: Pass source Kafka as a function parameter
    source.setConnectionString(sourceConnectionString);
    source.setPartitions(partitionCount);
    datastream.setSource(source);

    String destinationConnectionString = getKafkaURL(destinationTopic);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(destinationConnectionString);
    destination.setPartitions(partitionCount);
    datastream.setDestination(destination);
    datastream.setTransportProviderName(TRANSPORT_PROVIDER_NAME);

    /**
     * Create Brooklin Datastream with retries.
     */
    Exception lastException = null;
    for (int attempt = 1; attempt <= MAX_DATASTREAM_CREATION_ATTEMPTS; ++attempt) {
      try {
        client.createDatastream(datastream);
        client.waitTillDatastreamIsInitialized(datastream.getName(), Duration.ofSeconds(60).toMillis());
      } catch (Exception e) {
        lastException = e;
      }
      if (null == lastException || client.datastreamExists(name)) {
        lastException = null;
        break;
      }
    }
    if (null != lastException) {
      if (lastException instanceof InterruptedException) {
        logger.warn("Interrupted while waiting for datastream " + name + " to be initialized.", lastException);
      } else {
        logger.error("Error when creating datastream " + name, lastException);
        throw new VeniceException(lastException);
      }
    }
  }

  @Override
  void terminateReplicationInternal(String sourceTopic, String destinationTopic) {
    String name = datastreamName(sourceTopic, destinationTopic);
    if (client.datastreamExists(name)){
      client.deleteDatastream(name);
      try {
        client.waitTillDatastreamIsDeleted(name, Duration.ofSeconds(30).toMillis());
      } catch (InterruptedException e) {
        logger.warn("Interrupted while waiting for datastream " + name + " to be deleted.", e);
      } catch (Exception e) {
        if (e instanceof RestLiResponseException) {
          RestLiResponseException restLiResponseException = (RestLiResponseException) e;
          if (restLiResponseException.getStatus() == HttpResponseStatus.NOT_FOUND.code()) {
            logger.info("Brooklin returned a 404, meaning the datastream we intend to have deleted cannot be found. Good enough...");
            return;
          }
        }
        throw new VeniceException("Caught an exception from Brooklin client's waitTillDatastreamIsDeleted()", e);
      }
    } else {
      logger.error("Cannot delete brooklin replication stream named " + name + " because it does not exist");
    }
  }

  /**
   * Only used by tests
   */
  @Override
  public boolean doesReplicationExist(String sourceTopic, String destinationTopic) {
    String name = datastreamName(sourceTopic, destinationTopic);
    return client.datastreamExists(name);
  }

  /**
   * Callers to begin or terminate replication should be able to just specify the source and destination topics.
   * This method lets us use a reliable convention for turning source and destination into a datastream name
   * to use for referring to the datastream.
   * @param source
   * @param destination
   * @return
   */
  private String datastreamName(String source, String destination){
    return Arrays.asList(veniceCluster, source, destination).stream()
        .collect(Collectors.joining("__"));
  }

  public String getKafkaURL(String topic) {
    return (isSSLToKafka ? SSL_KAFKA_PREFIX : KAFKA_PPREFIX) + destKafkaBootstrapServers + "/" + topic;
  }
}
