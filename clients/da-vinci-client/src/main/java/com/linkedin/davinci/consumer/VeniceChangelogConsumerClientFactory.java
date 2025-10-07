package com.linkedin.davinci.consumer;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerClientFactory.class);
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new VeniceConcurrentHashMap<>();
  private final Map<String, VeniceChangelogConsumer> versionSpecificStoreClientMap = new VeniceConcurrentHashMap<>();
  private final Map<String, BootstrappingVeniceChangelogConsumer> storeBootstrappingClientMap =
      new VeniceConcurrentHashMap<>();

  private final MetricsRepository metricsRepository;

  private final ChangelogClientConfig globalChangelogClientConfig;

  private D2ControllerClient d2ControllerClient;

  private PubSubConsumerAdapter consumer;

  private PubSubMessageDeserializer pubSubMessageDeserializer;

  protected ViewClassGetter viewClassGetter;

  public VeniceChangelogConsumerClientFactory(
      ChangelogClientConfig globalChangelogClientConfig,
      MetricsRepository metricsRepository) {
    this.globalChangelogClientConfig = globalChangelogClientConfig;
    this.metricsRepository = metricsRepository;
    this.viewClassGetter = getDefaultViewClassGetter();
    this.pubSubMessageDeserializer = createPubSubMessageDeserializer(globalChangelogClientConfig);
  }

  protected void setD2ControllerClient(D2ControllerClient d2ControllerClient) {
    this.d2ControllerClient = d2ControllerClient;
  }

  protected void setConsumer(PubSubConsumerAdapter consumer) {
    this.consumer = consumer;
  }

  /**
   * Default method to create a {@link VeniceChangelogConsumer} given a storeName.
   */
  public <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(String storeName) {
    return getChangelogConsumer(storeName, null);
  }

  public <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(String storeName, String consumerId) {
    return getChangelogConsumer(storeName, consumerId, null);
  }

  /**
   * Creates a VeniceChangelogConsumer with consumer id. This is used to create multiple consumers so that
   * each consumer can only subscribe to certain partitions. Multiple such consumers can work in parallel.
   */
  public <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(
      String storeName,
      String consumerId,
      Class<V> valueClass) {
    return getChangelogConsumer(storeName, consumerId, valueClass, globalChangelogClientConfig.getViewName());
  }

  public <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(
      String storeName,
      String consumerId,
      Class<V> valueClass,
      String viewNameOverride) {
    String adjustedConsumerId;
    if (!StringUtils.isEmpty(viewNameOverride)) {
      if (StringUtils.isEmpty(consumerId)) {
        adjustedConsumerId = viewNameOverride;
      } else {
        adjustedConsumerId = consumerId + "-" + viewNameOverride;
      }
    } else {
      adjustedConsumerId = consumerId;
    }

    return storeClientMap.computeIfAbsent(suffixConsumerIdToStore(storeName, adjustedConsumerId), name -> {
      ChangelogClientConfig newStoreChangelogClientConfig =
          getNewStoreChangelogClientConfig(storeName).setSpecificValue(valueClass)
              .setConsumerName(name)
              .setViewName(viewNameOverride)
              .setIsStateful(false);
      String viewClass = getViewClass(newStoreChangelogClientConfig, storeName);
      String consumerName =
          suffixConsumerIdToStore(storeName + "-" + viewClass.getClass().getSimpleName(), adjustedConsumerId);
      if (viewClass.equals(ChangeCaptureView.class.getCanonicalName())) {
        // TODO: This is a little bit of a hack. This is to deal with the an issue where the before image change
        // capture topic doesn't follow the same naming convention as view topics.
        newStoreChangelogClientConfig.setIsBeforeImageView(true);
        return new VeniceChangelogConsumerImpl(
            newStoreChangelogClientConfig,
            consumer != null
                ? consumer
                : getPubSubConsumer(newStoreChangelogClientConfig, pubSubMessageDeserializer, consumerName),
            pubSubMessageDeserializer);
      }
      return new VeniceAfterImageConsumerImpl(
          newStoreChangelogClientConfig,
          consumer != null
              ? consumer
              : getPubSubConsumer(newStoreChangelogClientConfig, pubSubMessageDeserializer, consumerName),
          pubSubMessageDeserializer);
    });
  }

  private String suffixConsumerIdToStore(String storeName, String consumerId) {
    return StringUtils.isEmpty(consumerId) ? storeName : storeName + "-" + consumerId;
  }

  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(String storeName) {
    return getBootstrappingChangelogConsumer(storeName, null);
  }

  /**
   * Use this if you're using the experimental client
   * @param keyClass The {@link SpecificRecord} class for your key
   * @param valueClass The {@link SpecificRecord} class for your value
   * @param valueSchema The {@link Schema} for your values
   */
  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(
      String storeName,
      String consumerId,
      Class<K> keyClass,
      Class<V> valueClass,
      Schema valueSchema) {
    String consumerName = suffixConsumerIdToStore(storeName, consumerId);

    return storeBootstrappingClientMap.computeIfAbsent(consumerName, name -> {
      ChangelogClientConfig newStoreChangelogClientConfig =
          getNewStoreChangelogClientConfig(storeName).setSpecificKey(keyClass)
              .setSpecificValue(valueClass)
              .setSpecificValueSchema(valueSchema)
              .setConsumerName(consumerName)
              .setIsStateful(true);

      if (globalChangelogClientConfig.isExperimentalClientEnabled()) {
        return new VeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>(newStoreChangelogClientConfig);
      } else {
        return new LocalBootstrappingVeniceChangelogConsumer<K, V>(
            newStoreChangelogClientConfig,
            consumer != null
                ? consumer
                : getPubSubConsumer(newStoreChangelogClientConfig, pubSubMessageDeserializer, consumerName),
            pubSubMessageDeserializer,
            consumerId);
      }
    });
  }

  /**
   * Creates a BootstrappingVeniceChangelogConsumer with consumer id. This is used to create multiple
   * consumers so that each consumer can only subscribe to certain partitions.
   */
  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(
      String storeName,
      String consumerId,
      Class<V> valueClass) {
    return getBootstrappingChangelogConsumer(storeName, consumerId, null, valueClass, null);
  }

  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(
      String storeName,
      String consumerId) {
    return getBootstrappingChangelogConsumer(storeName, consumerId, null);
  }

  /**
   * Subscribes to a specific version of a Venice store. This is only intended for internal use.
   */
  public <K, V> VeniceChangelogConsumer<K, V> getVersionSpecificChangelogConsumer(
      String storeName,
      int storeVersion,
      String consumerId) {
    String consumerName = suffixConsumerIdToStore(storeName, consumerId);

    return versionSpecificStoreClientMap.computeIfAbsent(consumerName + "v_" + storeVersion, name -> {
      ChangelogClientConfig newStoreChangelogClientConfig =
          getNewStoreChangelogClientConfig(storeName).setConsumerName(consumerName)
              .setStoreVersion(storeVersion)
              .setIsStateful(false);

      return new VeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>(newStoreChangelogClientConfig);
    });
  }

  private ChangelogClientConfig getNewStoreChangelogClientConfig(String storeName) {
    ChangelogClientConfig newStoreChangelogClientConfig =
        ChangelogClientConfig.cloneConfig(globalChangelogClientConfig).setStoreName(storeName);
    newStoreChangelogClientConfig.getInnerClientConfig().setMetricsRepository(metricsRepository);

    D2ControllerClient d2ControllerClient;
    if (this.d2ControllerClient != null) {
      d2ControllerClient = this.d2ControllerClient;
    } else if (newStoreChangelogClientConfig.getD2Client() != null) {
      d2ControllerClient = D2ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          globalChangelogClientConfig.getControllerD2ServiceName(),
          globalChangelogClientConfig.getControllerRequestRetryCount(),
          newStoreChangelogClientConfig.getD2Client());
    } else {
      throw new IllegalArgumentException("D2Client should be set, please check the config");
    }
    newStoreChangelogClientConfig.setD2ControllerClient(d2ControllerClient);
    if (newStoreChangelogClientConfig.getSchemaReader() == null) {
      newStoreChangelogClientConfig
          .setSchemaReader(ClientFactory.getSchemaReader(newStoreChangelogClientConfig.getInnerClientConfig()));
    }

    return newStoreChangelogClientConfig;
  }

  private String getViewClass(ChangelogClientConfig newStoreChangelogClientConfig, String storeName) {
    // TODO: This is a redundant controller query. Need to condense it with the storeInfo query that happens
    // inside the change capture client itself
    String viewClass =
        newStoreChangelogClientConfig.getViewName() == null ? "" : newStoreChangelogClientConfig.getViewName();

    if (!viewClass.isEmpty()) {
      viewClass = getViewClass(
          storeName,
          newStoreChangelogClientConfig.getViewName(),
          newStoreChangelogClientConfig.getD2ControllerClient(),
          globalChangelogClientConfig.getControllerRequestRetryCount());
    }

    return viewClass;
  }

  protected static PubSubConsumerAdapter getPubSubConsumer(
      ChangelogClientConfig changelogClientConfig,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName) {
    PubSubConsumerAdapterContext context = new PubSubConsumerAdapterContext.Builder().setConsumerName(consumerName)
        .setVeniceProperties(new VeniceProperties(changelogClientConfig.getConsumerProperties()))
        .setPubSubMessageDeserializer(pubSubMessageDeserializer)
        .setPubSubTopicRepository(changelogClientConfig.getPubSubContext().getPubSubTopicRepository())
        .setPubSubPositionTypeRegistry(changelogClientConfig.getPubSubContext().getPubSubPositionTypeRegistry())
        .build();
    return changelogClientConfig.getPubSubConsumerAdapterFactory().create(context);
  }

  /**
   * Create a {@link PubSubMessageDeserializer} for changelog consumption.
   *
   * <p>Behavior:
   * <ul>
   *   <li>If {@code kme.schema.reader.for.schema.evolution.enabled} is true
   *       and a non null D2 client is present in the config, build a deserializer that uses
   *       a KME backed {@link SchemaReader} for schema evolution during message decoding.</li>
   *   <li>Otherwise, fall back to {@link PubSubMessageDeserializer#createDefaultDeserializer()}.</li>
   * </ul>
   */
  @VisibleForTesting
  static PubSubMessageDeserializer createPubSubMessageDeserializer(final ChangelogClientConfig changelogClientConfig) {
    Objects.requireNonNull(changelogClientConfig, "changelogClientConfig");
    VeniceProperties properties = new VeniceProperties(changelogClientConfig.getConsumerProperties());
    D2Client d2Client = changelogClientConfig.getD2Client();
    boolean kmeEnabled = properties.getBoolean(ConfigKeys.KME_SCHEMA_READER_FOR_SCHEMA_EVOLUTION_ENABLED, true);

    if (kmeEnabled && d2Client != null) {
      LOGGER.info("Creating KME reader backed PubSubMessageDeserializer");
      // Create a SchemaReader for the KME system store to enable schema evolution during deserialization.
      ClientConfig innerClient = ClientConfig.cloneConfig(changelogClientConfig.getInnerClientConfig())
          .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName());
      SchemaReader schemaReader = ClientFactory.getSchemaReader(innerClient, null);
      KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer();
      kafkaValueSerializer.setSchemaReader(schemaReader);
      return new PubSubMessageDeserializer(
          kafkaValueSerializer,
          new LandFillObjectPool<>(KafkaMessageEnvelope::new),
          new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    }

    LOGGER.info(
        "KME reader backed PubSubMessageDeserializer is disabled, falling back to default deserializer. kmeEnabled: {}, isD2ClientPresent: {}",
        kmeEnabled,
        d2Client != null);
    // Safe default when KME is disabled or D2 is unavailable.
    return PubSubMessageDeserializer.createDefaultDeserializer();
  }

  private String getViewClass(String storeName, String viewName, D2ControllerClient d2ControllerClient, int retries) {
    return this.viewClassGetter.apply(storeName, viewName, d2ControllerClient, retries);
  }

  private ViewClassGetter getDefaultViewClassGetter() {
    return (String storeName, String viewName, D2ControllerClient d2ControllerClient, int retries) -> {
      StoreResponse response =
          d2ControllerClient.retryableRequest(retries, controllerClient -> controllerClient.getStore(storeName));
      if (response.isError()) {
        throw new VeniceException(
            "Couldn't retrieve store information when building change capture client for store " + storeName);
      }
      ViewConfig viewConfig = response.getStore().getViewConfigs().get(viewName);
      if (viewConfig == null) {
        throw new VeniceException(
            "Couldn't retrieve store view information when building change capture client for store " + storeName
                + " viewName " + viewName);
      }
      return viewConfig.getViewClassName();
    };
  }

  protected void setViewClassGetter(ViewClassGetter viewClassGetter) {
    this.viewClassGetter = viewClassGetter;
  }

  @FunctionalInterface
  public interface ViewClassGetter {
    String apply(String storeName, String viewName, D2ControllerClient d2ControllerClient, int retries);
  }
}
