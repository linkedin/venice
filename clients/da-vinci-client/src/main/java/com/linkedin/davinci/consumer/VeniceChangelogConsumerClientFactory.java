package com.linkedin.davinci.consumer;

import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.StringUtils;


public class VeniceChangelogConsumerClientFactory {
  private final Map<String, VeniceChangelogConsumer> storeClientMap = new VeniceConcurrentHashMap<>();
  private final Map<String, BootstrappingVeniceChangelogConsumer> storeBootstrappingClientMap =
      new VeniceConcurrentHashMap<>();

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
          getNewStoreChangelogClientConfig(storeName).setSpecificValue(valueClass);
      newStoreChangelogClientConfig.setConsumerName(name);
      newStoreChangelogClientConfig.setViewName(viewNameOverride);
      String viewClass = getViewClass(newStoreChangelogClientConfig, storeName);
      String consumerName =
          suffixConsumerIdToStore(storeName + "-" + viewClass.getClass().getSimpleName(), adjustedConsumerId);
      if (viewClass.equals(ChangeCaptureView.class.getCanonicalName())) {
        // TODO: This is a little bit of a hack. This is to deal with the an issue where the before image change
        // capture topic doesn't follow the same naming convention as view topics.
        newStoreChangelogClientConfig.setIsBeforeImageView(true);
        return new VeniceChangelogConsumerImpl(
            newStoreChangelogClientConfig,
            consumer != null ? consumer : getPubSubConsumer(newStoreChangelogClientConfig, consumerName));
      }
      return new VeniceAfterImageConsumerImpl(
          newStoreChangelogClientConfig,
          consumer != null ? consumer : getPubSubConsumer(newStoreChangelogClientConfig, consumerName));
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
              .setConsumerName(consumerName);

      if (globalChangelogClientConfig.isExperimentalClientEnabled()) {
        return new BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<K, V>(
            newStoreChangelogClientConfig);
      } else {
        return new LocalBootstrappingVeniceChangelogConsumer<K, V>(
            newStoreChangelogClientConfig,
            consumer != null
                ? consumer
                : VeniceChangelogConsumerClientFactory.getPubSubConsumer(newStoreChangelogClientConfig, consumerName),
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
      String consumerName) {
    PubSubConsumerAdapterContext context = new PubSubConsumerAdapterContext.Builder().setConsumerName(consumerName)
        .setVeniceProperties(new VeniceProperties(changelogClientConfig.getConsumerProperties()))
        .setPubSubMessageDeserializer(changelogClientConfig.getPubSubMessageDeserializer())
        .setPubSubTopicRepository(changelogClientConfig.getPubSubTopicRepository())
        .setPubSubPositionTypeRegistry(changelogClientConfig.getPubSubPositionTypeRegistry())
        .build();
    return changelogClientConfig.getPubSubConsumerAdapterFactory().create(context);
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
