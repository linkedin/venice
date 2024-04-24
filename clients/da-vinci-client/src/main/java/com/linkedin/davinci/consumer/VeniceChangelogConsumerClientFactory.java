package com.linkedin.davinci.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;


public class VeniceChangelogConsumerClientFactory {
  private static final PubSubConsumerAdapterFactory kafkaConsumerAdapterFactory =
      new ApacheKafkaConsumerAdapterFactory();
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new VeniceConcurrentHashMap<>();

  private final MetricsRepository metricsRepository;

  private final ChangelogClientConfig globalChangelogClientConfig;

  private D2ControllerClient d2ControllerClient;

  private PubSubConsumerAdapter consumer;

  protected ViewClassGetter viewClassGetter;

  // TODO: Add PubSubConsumerFactory into the constructor, so that client can choose specific Pub Sub system's consumer.
  public VeniceChangelogConsumerClientFactory(
      ChangelogClientConfig globalChangelogClientConfig,
      MetricsRepository metricsRepository) {
    this.globalChangelogClientConfig = globalChangelogClientConfig;
    this.metricsRepository = metricsRepository;
    this.viewClassGetter = getDefaultViewClassGetter();
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

  /**
   * Creates a VeniceChangelogConsumer with consumer id. This is used to create multiple consumers so that
   * each consumer can only subscribe to certain partitions. Multiple such consumers can work in parallel.
   */
  public <K, V> VeniceChangelogConsumer<K, V> getChangelogConsumer(String storeName, String consumerId) {
    return storeClientMap.computeIfAbsent(suffixConsumerIdToStore(storeName, consumerId), name -> {
      ChangelogClientConfig newStoreChangelogClientConfig = getNewStoreChangelogClientConfig(storeName);
      newStoreChangelogClientConfig.setConsumerName(name);
      String viewClass = getViewClass(newStoreChangelogClientConfig, storeName);
      String consumerName = suffixConsumerIdToStore(storeName + "-" + viewClass.getClass().getSimpleName(), consumerId);
      if (viewClass.equals(ChangeCaptureView.class.getCanonicalName())) {
        return new VeniceChangelogConsumerImpl(
            newStoreChangelogClientConfig,
            consumer != null
                ? consumer
                : getConsumer(newStoreChangelogClientConfig.getConsumerProperties(), consumerName));
      }
      return new VeniceAfterImageConsumerImpl(
          newStoreChangelogClientConfig,
          consumer != null
              ? consumer
              : getConsumer(newStoreChangelogClientConfig.getConsumerProperties(), consumerName));
    });
  }

  private String suffixConsumerIdToStore(String storeName, String consumerId) {
    return StringUtils.isEmpty(consumerId) ? storeName : storeName + "-" + consumerId;
  }

  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(String storeName) {
    return getBootstrappingChangelogConsumer(storeName, null);
  }

  /**
   * Creates a BootstrappingVeniceChangelogConsumer with consumer id. This is used to create multiple
   * consumers so that each consumer can only subscribe to certain partitions.
   */
  public <K, V> BootstrappingVeniceChangelogConsumer<K, V> getBootstrappingChangelogConsumer(
      String storeName,
      String consumerId) {
    return (BootstrappingVeniceChangelogConsumer<K, V>) storeClientMap
        .computeIfAbsent(suffixConsumerIdToStore(storeName, consumerId), name -> {
          ChangelogClientConfig newStoreChangelogClientConfig = getNewStoreChangelogClientConfig(storeName);
          String viewClass = getViewClass(newStoreChangelogClientConfig, storeName);
          String consumerName =
              suffixConsumerIdToStore(storeName + "-" + viewClass.getClass().getSimpleName(), consumerId);
          return new LocalBootstrappingVeniceChangelogConsumer(
              newStoreChangelogClientConfig,
              consumer != null
                  ? consumer
                  : getConsumer(newStoreChangelogClientConfig.getConsumerProperties(), consumerName),
              consumerId);
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
      d2ControllerClient = D2ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          globalChangelogClientConfig.getControllerD2ServiceName(),
          globalChangelogClientConfig.getLocalD2ZkHosts(),
          Optional.ofNullable(newStoreChangelogClientConfig.getInnerClientConfig().getSslFactory()),
          globalChangelogClientConfig.getControllerRequestRetryCount());
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

  static PubSubConsumerAdapter getConsumer(Properties consumerProps, String consumerName) {
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    return kafkaConsumerAdapterFactory
        .create(new VeniceProperties(consumerProps), false, pubSubMessageDeserializer, consumerName);
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
