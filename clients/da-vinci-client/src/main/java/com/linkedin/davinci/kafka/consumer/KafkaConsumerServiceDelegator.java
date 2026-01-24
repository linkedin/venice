package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This delegator impl is used to distribute different partition requests into different consumer service.
 * When {#link ConfigKeys#SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED} is off, this class
 * will always return the default consumer service.
 * When the option is on, it will return the dedicated consumer service when the topic partition belongs
 * to a Real-time topic and the corresponding store has active/active or write compute enabled.
 * The reason to use dedicated consumer pool for leader replicas of active/active or write compute stores is
 * that handling the writes before putting into the drainer queue is too expensive comparing to others.
 */
public class KafkaConsumerServiceDelegator extends AbstractKafkaConsumerService {
  private final Function<String, Boolean> isAAWCStoreFunc;

  /**
   * Map from {@link TopicPartitionForIngestion} to the {@link KafkaConsumerService} assigned to it.
   * {@link TopicPartitionForIngestion} wrapping version topic and pub-sub topic partition to identify a unique partition
   * in the specific host. For example, the server host may ingestion same real-time pub-sub topic partition from
   * different version topics, and we should avoid sharing the same consumer for them.
   */
  private final Map<TopicPartitionForIngestion, KafkaConsumerService> topicPartitionToConsumerService =
      new VeniceConcurrentHashMap<>();
  private final List<KafkaConsumerService> consumerServices;

  /**
   * The reason to introduce this cache layer is that write-compute is a store-level feature, which means
   * it can change in the lifetime of a particular store version, which might lead this class to pick
   * up a wrong consumer service.
   * For example
   * 1. StoreA doesn't have write compute enabled.
   * 2. StoreA leader gets assigned to default consumer pool.
   * 3. StoreA enables write compute.
   * 4. Without this cache, the consumer operations of the same partition will be forwarded to dedicated consumer pool, which is wrong.
   */
  private final VeniceConcurrentHashMap<String, Boolean> storeVersionAAWCFlagMap = new VeniceConcurrentHashMap<>();
  private final ConsumerPoolStrategy consumerPoolStrategy;
  private final KafkaConsumerServiceBuilder consumerServiceConstructor;
  private final VeniceServerConfig serverConfig;

  interface KafkaConsumerServiceBuilder {
    KafkaConsumerService apply(int poolSize, ConsumerPoolType poolType);
  }

  private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerServiceDelegator.class);

  public KafkaConsumerServiceDelegator(
      VeniceServerConfig serverConfig,
      KafkaConsumerServiceBuilder consumerServiceConstructor,
      Function<String, Boolean> isAAWCStoreFunc) {
    this.serverConfig = serverConfig;
    this.consumerServiceConstructor = consumerServiceConstructor;
    this.isAAWCStoreFunc = vt -> storeVersionAAWCFlagMap.computeIfAbsent(vt, ignored -> isAAWCStoreFunc.apply(vt));
    ConsumerPoolStrategyType consumerPoolStrategyType = serverConfig.getConsumerPoolStrategyType();

    if (consumerPoolStrategyType == ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION) {
      if (serverConfig.isResubscriptionTriggeredByVersionIngestionContextChangeEnabled()) {
        consumerPoolStrategy = new CurrentVersionConsumerPoolStrategy();
      } else {
        throw new VeniceException(
            "Resubscription should be enabled with consumer pool strategy: " + consumerPoolStrategyType);
      }
    } else {
      consumerPoolStrategy = new DefaultConsumerPoolStrategy();
    }
    LOGGER.info("Initializing Consumer Service Delegator with Consumer pool strategy: {}", consumerPoolStrategyType);

    this.consumerServices = consumerPoolStrategy.getConsumerServices();
  }

  private KafkaConsumerService assignKafkaConsumerServiceFor(
      PartitionReplicaIngestionContext topicPartitionReplicaRole) {
    TopicPartitionForIngestion topicPartitionForIngestion = new TopicPartitionForIngestion(
        topicPartitionReplicaRole.getVersionTopic(),
        topicPartitionReplicaRole.getPubSubTopicPartition());
    return topicPartitionToConsumerService.computeIfAbsent(
        topicPartitionForIngestion,
        pair -> consumerPoolStrategy.delegateKafkaConsumerServiceFor(topicPartitionReplicaRole));
  }

  private KafkaConsumerService getKafkaConsumerService(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    TopicPartitionForIngestion versionTopicPartitionPair = new TopicPartitionForIngestion(versionTopic, topicPartition);
    return topicPartitionToConsumerService.get(versionTopicPartitionPair);
  }

  /**
    * There might be no consumer service found for the version topic and topic partition, which means the topic partition
   *  is not subscribed. In this case, we should return null. Caller should check the return value and handle it properly.
   */
  @Override
  public SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, topicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
    } else {
      return null;
    }
  }

  @Override
  public SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.assignConsumerFor(versionTopic, pubSubTopicPartition);
    } else {
      LOGGER.error(
          "No consumer service found for version topic {} and partition {} when assigning consumer.",
          versionTopic,
          pubSubTopicPartition);
      return null;
    }
  }

  @Override
  public void unsubscribeAll(PubSubTopic versionTopic) {
    consumerServices.forEach(kafkaConsumerService -> kafkaConsumerService.unsubscribeAll(versionTopic));
    topicPartitionToConsumerService.entrySet().removeIf(entry -> entry.getKey().getVersionTopic().equals(versionTopic));
    storeVersionAAWCFlagMap.remove(versionTopic.getName());
  }

  @Override
  public void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition, long timeoutMs) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      kafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition, timeoutMs);
      topicPartitionToConsumerService.remove(new TopicPartitionForIngestion(versionTopic, pubSubTopicPartition));
    } else {
      LOGGER.warn(
          "No consumer service found for version topic {} and partition {} when unsubscribing.",
          versionTopic,
          pubSubTopicPartition);
    }
  }

  @Override
  public void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub) {
    consumerServices
        .forEach(kafkaConsumerService -> kafkaConsumerService.batchUnsubscribe(versionTopic, topicPartitionsToUnSub));
    for (PubSubTopicPartition pubSubTopicPartition: topicPartitionsToUnSub) {
      topicPartitionToConsumerService.remove(new TopicPartitionForIngestion(versionTopic, pubSubTopicPartition));
    }
  }

  @Override
  public boolean hasAnySubscriptionFor(PubSubTopic versionTopic) {
    for (KafkaConsumerService kafkaConsumerService: this.consumerServices) {
      if (kafkaConsumerService.hasAnySubscriptionFor(versionTopic)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long getMaxElapsedTimeMSSinceLastPollInConsumerPool() {
    long max = -1;
    for (KafkaConsumerService kafkaConsumerService: this.consumerServices) {
      max = Math.max(max, kafkaConsumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool());
    }
    return max;
  }

  @Override
  public void startConsumptionIntoDataReceiver(
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      PubSubPosition lastReadPosition,
      ConsumedDataReceiver<List<DefaultPubSubMessage>> consumedDataReceiver,
      boolean inclusive) {
    assignKafkaConsumerServiceFor(partitionReplicaIngestionContext).startConsumptionIntoDataReceiver(
        partitionReplicaIngestionContext,
        lastReadPosition,
        consumedDataReceiver,
        inclusive);
  }

  @Override
  public long getLatestOffsetBasedOnMetrics(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.getLatestOffsetBasedOnMetrics(versionTopic, pubSubTopicPartition);
    } else {
      return -1;
    }
  }

  @Override
  public Map<PubSubTopicPartition, TopicPartitionIngestionInfo> getIngestionInfoFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      boolean respectRedundantLoggingFilter) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService
          .getIngestionInfoFor(versionTopic, pubSubTopicPartition, respectRedundantLoggingFilter);
    } else {
      LOGGER.warn(
          "No consumer service found for version topic {} and partition {} when fetching ingestion info"
              + " from consumer.",
          versionTopic,
          pubSubTopicPartition);
      return Collections.emptyMap();
    }
  }

  @Override
  public Map<PubSubTopicPartition, Long> getStaleTopicPartitions(long thresholdTimestamp) {
    Map<PubSubTopicPartition, Long> consolidatedMap = new HashMap<>();
    for (KafkaConsumerService kafkaConsumerService: consumerServices) {
      Map<PubSubTopicPartition, Long> staleTopicPartitions =
          kafkaConsumerService.getStaleTopicPartitions(thresholdTimestamp);
      for (Map.Entry<PubSubTopicPartition, Long> entry: staleTopicPartitions.entrySet()) {
        if (consolidatedMap.containsKey(entry.getKey())) {
          // Keep the entry with older timestamp.
          long subscribeTimestamp = consolidatedMap.get(entry.getKey());
          if (entry.getValue() < subscribeTimestamp) {
            consolidatedMap.put(entry.getKey(), entry.getValue());
          }
        } else {
          consolidatedMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return consolidatedMap;
  }

  @Override
  public boolean startInner() throws Exception {
    consumerServices.forEach(KafkaConsumerService::start);
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    for (KafkaConsumerService kafkaConsumerService: consumerServices) {
      kafkaConsumerService.stop();
    }
  }

  public abstract class ConsumerPoolStrategy {
    protected final List<KafkaConsumerService> consumerServices = new ArrayList<>();

    protected abstract KafkaConsumerService delegateKafkaConsumerServiceFor(
        PartitionReplicaIngestionContext topicPartitionReplicaRole);

    protected List<KafkaConsumerService> getConsumerServices() {
      return consumerServices;
    }
  }

  public class DefaultConsumerPoolStrategy extends ConsumerPoolStrategy {
    protected final KafkaConsumerService defaultConsumerService;

    public DefaultConsumerPoolStrategy() {
      defaultConsumerService = consumerServiceConstructor
          .apply(serverConfig.getConsumerPoolSizePerKafkaCluster(), ConsumerPoolType.REGULAR_POOL);
      consumerServices.add(defaultConsumerService);
    }

    @Override
    public KafkaConsumerService delegateKafkaConsumerServiceFor(
        PartitionReplicaIngestionContext topicPartitionReplicaRole) {
      return defaultConsumerService;
    }

    @Override
    public List<KafkaConsumerService> getConsumerServices() {
      return consumerServices;
    }
  }

  public class CurrentVersionConsumerPoolStrategy extends ConsumerPoolStrategy {
    private final KafkaConsumerService consumerServiceForCurrentVersionAAWCLeader;
    private final KafkaConsumerService consumerServiceForCurrentVersionSepRTLeader;
    private final KafkaConsumerService consumerServiceForCurrentVersionNonAAWCLeader;
    private final KafkaConsumerService consumerServiceForNonCurrentVersionAAWCLeader;
    private final KafkaConsumerService consumerServiceNonCurrentNonAAWCLeader;

    public CurrentVersionConsumerPoolStrategy() {
      this.consumerServiceForCurrentVersionAAWCLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForCurrentVersionAAWCLeader(),
          ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL);
      consumerServices.add(consumerServiceForCurrentVersionAAWCLeader);
      this.consumerServiceForCurrentVersionSepRTLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForCurrentVersionSepRTLeader(),
          ConsumerPoolType.CURRENT_VERSION_SEP_RT_LEADER_POOL);
      consumerServices.add(consumerServiceForCurrentVersionSepRTLeader);
      this.consumerServiceForCurrentVersionNonAAWCLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForCurrentVersionNonAAWCLeader(),
          ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL);
      consumerServices.add(consumerServiceForCurrentVersionNonAAWCLeader);
      this.consumerServiceForNonCurrentVersionAAWCLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForNonCurrentVersionAAWCLeader(),
          ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL);
      consumerServices.add(consumerServiceForNonCurrentVersionAAWCLeader);
      this.consumerServiceNonCurrentNonAAWCLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForNonCurrentVersionNonAAWCLeader(),
          ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL);
      consumerServices.add(consumerServiceNonCurrentNonAAWCLeader);
    }

    @Override
    public KafkaConsumerService delegateKafkaConsumerServiceFor(
        PartitionReplicaIngestionContext topicPartitionReplicaRole) {
      VersionRole versionRole = topicPartitionReplicaRole.getVersionRole();
      PartitionReplicaIngestionContext.WorkloadType workloadType = topicPartitionReplicaRole.getWorkloadType();
      PubSubTopicPartition pubSubTopicPartition = topicPartitionReplicaRole.getPubSubTopicPartition();
      PubSubTopic pubSubTopic = pubSubTopicPartition.getPubSubTopic();
      /**
       * The logic to assign consumer service is with these 3 dimensions:
       * 1. If the version role is current.
       * 2. If the workload type is active-active leader and write-computer leader.
       * 3. If the replica is ready to serve.
       */
      if (versionRole.equals(VersionRole.CURRENT) && topicPartitionReplicaRole.isReadyToServe()) {
        if (workloadType.equals(PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE)
            && pubSubTopic.isRealTime()) {
          return pubSubTopic.isSeparateRealTimeTopic()
              ? consumerServiceForCurrentVersionSepRTLeader
              : consumerServiceForCurrentVersionAAWCLeader;
        } else {
          return consumerServiceForCurrentVersionNonAAWCLeader;
        }
      } else {
        if (workloadType.equals(PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE)
            && pubSubTopic.isRealTime()) {
          return consumerServiceForNonCurrentVersionAAWCLeader;
        } else {
          return consumerServiceNonCurrentNonAAWCLeader;
        }
      }
    }

    @Override
    public List<KafkaConsumerService> getConsumerServices() {
      return consumerServices;
    }
  }

  public enum ConsumerPoolStrategyType {
    DEFAULT, CURRENT_VERSION_PRIORITIZATION
  }
}
