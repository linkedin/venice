package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * This delegator impl is used to distribute different partition requests into different consumer service.
 * When {@text ConfigKeys#SERVER_DEDICATED_CONSUMER_POOL_FOR_AA_WC_LEADER_ENABLED} is off, this class
 * will always return the default consumer service.
 * When the option is on, it will return the dedicated consumer service when the topic partition belongs
 * to a Real-time topic and the corresponding store has active/active or write compute enabled.
 * The reason to use dedicated consumer pool for leader replicas of active/active or write compute stores is
 * that handling the writes before putting into the drainer queue is too expensive comparing to others.
 */
public class KafkaConsumerServiceDelegator extends AbstractKafkaConsumerService {
  private final Function<String, Boolean> isAAWCStoreFunc;
  // Map from a pair of <Version Topic, PubSubTopicPartition> to the consumer service assigned to it.
  private final Map<Pair<PubSubTopic, PubSubTopicPartition>, KafkaConsumerService> vtTopicPartitionPairToConsumerService =
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
  private final BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor;
  private final VeniceServerConfig serverConfig;

  public KafkaConsumerServiceDelegator(
      VeniceServerConfig serverConfig,
      BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor,
      Function<String, Boolean> isAAWCStoreFunc) {

    this.serverConfig = serverConfig;
    this.consumerServiceConstructor = consumerServiceConstructor;
    this.isAAWCStoreFunc = vt -> storeVersionAAWCFlagMap.computeIfAbsent(vt, ignored -> isAAWCStoreFunc.apply(vt));
    ConsumerPoolStrategyType consumerPoolStrategyType = serverConfig.getConsumerPoolStrategyType();

    // For backward compatibility, if the dedicated consumer pool for AA/WC leader is enabled, we will use it.
    // TODO: Remove this block after new pooling strategy is verified in production environment.
    if (serverConfig.isDedicatedConsumerPoolForAAWCLeaderEnabled()) {
      this.consumerPoolStrategy = new AAOrWCLeaderConsumerPoolStrategy();
    } else {
      switch (consumerPoolStrategyType) {
        case AA_OR_WC_LEADER_DEDICATED:
          consumerPoolStrategy = new AAOrWCLeaderConsumerPoolStrategy();
          break;
        case CURRENT_VERSION_PRIORITIZATION:
          consumerPoolStrategy = new CurrentVersionConsumerPoolStrategy();
          break;
        default:
          consumerPoolStrategy = new DefaultConsumerPoolStrategy();
      }
    }
    this.consumerServices = consumerPoolStrategy.getConsumerServices();
  }

  private KafkaConsumerService assignKafkaConsumerServiceFor(
      PubSubTopic versionTopic,
      PartitionReplicaIngestionContext topicPartitionReplicaRole) {
    Pair<PubSubTopic, PubSubTopicPartition> versionTopicPartitionPair =
        new Pair<>(versionTopic, topicPartitionReplicaRole.getPubSubTopicPartition());
    return vtTopicPartitionPairToConsumerService.computeIfAbsent(
        versionTopicPartitionPair,
        pair -> consumerPoolStrategy.delegateKafkaConsumerServiceFor(versionTopic, topicPartitionReplicaRole));
  }

  private KafkaConsumerService getKafkaConsumerService(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    Pair<PubSubTopic, PubSubTopicPartition> versionTopicPartitionPair = new Pair<>(versionTopic, topicPartition);
    return vtTopicPartitionPairToConsumerService.get(versionTopicPartitionPair);
  }

  @Override
  public SharedKafkaConsumer getConsumerAssignedToVersionTopicPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    return getKafkaConsumerService(versionTopic, topicPartition)
        .getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
  }

  @Override
  public SharedKafkaConsumer assignConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition topicPartition) {
    return getKafkaConsumerService(versionTopic, topicPartition).assignConsumerFor(versionTopic, topicPartition);
  }

  @Override
  public void unsubscribeAll(PubSubTopic versionTopic) {
    consumerServices.forEach(kafkaConsumerService -> kafkaConsumerService.unsubscribeAll(versionTopic));
    vtTopicPartitionPairToConsumerService.entrySet().removeIf(entry -> entry.getKey().getFirst().equals(versionTopic));
    storeVersionAAWCFlagMap.remove(versionTopic.getName());
  }

  @Override
  public void unSubscribe(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      kafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition);
      vtTopicPartitionPairToConsumerService.remove(new Pair<>(versionTopic, pubSubTopicPartition));
    }
  }

  @Override
  public void batchUnsubscribe(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionsToUnSub) {
    consumerServices
        .forEach(kafkaConsumerService -> kafkaConsumerService.batchUnsubscribe(versionTopic, topicPartitionsToUnSub));
    for (PubSubTopicPartition pubSubTopicPartition: topicPartitionsToUnSub) {
      vtTopicPartitionPairToConsumerService.remove(new Pair<>(versionTopic, pubSubTopicPartition));
    }
  }

  @Override
  public boolean hasAnySubscriptionFor(PubSubTopic versionTopic) {
    return consumerServices.stream()
        .anyMatch(kafkaConsumerService -> kafkaConsumerService.hasAnySubscriptionFor(versionTopic));
  }

  @Override
  public long getMaxElapsedTimeMSSinceLastPollInConsumerPool() {
    return Collections.max(
        consumerServices.stream()
            .map(KafkaConsumerService::getMaxElapsedTimeMSSinceLastPollInConsumerPool)
            .collect(Collectors.toList()));
  }

  @Override
  public void startConsumptionIntoDataReceiver(
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      long lastReadOffset,
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver) {
    PubSubTopic versionTopic = consumedDataReceiver.destinationIdentifier();
    assignKafkaConsumerServiceFor(versionTopic, partitionReplicaIngestionContext)
        .startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, lastReadOffset, consumedDataReceiver);
  }

  @Override
  public long getOffsetLagBasedOnMetrics(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.getOffsetLagBasedOnMetrics(versionTopic, pubSubTopicPartition);
    }
    return -1;
  }

  @Override
  public long getLatestOffsetBasedOnMetrics(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.getLatestOffsetBasedOnMetrics(versionTopic, pubSubTopicPartition);
    }
    return -1;
  }

  @Override
  public Map<PubSubTopicPartition, TopicPartitionIngestionInfo> getIngestionInfoFromConsumer(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    KafkaConsumerService kafkaConsumerService = getKafkaConsumerService(versionTopic, pubSubTopicPartition);
    if (kafkaConsumerService != null) {
      return kafkaConsumerService.getIngestionInfoFromConsumer(versionTopic, pubSubTopicPartition);
    }
    return Collections.emptyMap();
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

  interface ConsumerPoolStrategy {
    KafkaConsumerService delegateKafkaConsumerServiceFor(
        PubSubTopic versionTopic,
        PartitionReplicaIngestionContext topicPartitionReplicaRole);

    List<KafkaConsumerService> getConsumerServices();
  }

  public class DefaultConsumerPoolStrategy implements ConsumerPoolStrategy {
    protected final KafkaConsumerService defaultConsumerService;

    public DefaultConsumerPoolStrategy() {
      defaultConsumerService = consumerServiceConstructor.apply(serverConfig.getConsumerPoolSizePerKafkaCluster(), "");
    }

    @Override
    public KafkaConsumerService delegateKafkaConsumerServiceFor(
        PubSubTopic versionTopic,
        PartitionReplicaIngestionContext topicPartitionReplicaRole) {
      return defaultConsumerService;
    }

    @Override
    public List<KafkaConsumerService> getConsumerServices() {
      return Collections.singletonList(defaultConsumerService);
    }
  }

  public class AAOrWCLeaderConsumerPoolStrategy extends DefaultConsumerPoolStrategy {
    private final KafkaConsumerService dedicatedConsumerService;

    public AAOrWCLeaderConsumerPoolStrategy() {
      super();
      dedicatedConsumerService = consumerServiceConstructor
          .apply(serverConfig.getDedicatedConsumerPoolSizeForAAWCLeader(), "_for_aa_wc_leader");
    }

    @Override
    public KafkaConsumerService delegateKafkaConsumerServiceFor(
        PubSubTopic versionTopic,
        PartitionReplicaIngestionContext topicPartitionReplicaRole) {
      PubSubTopicPartition topicPartition = topicPartitionReplicaRole.getPubSubTopicPartition();
      if (isAAWCStoreFunc.apply(versionTopic.getName()) && topicPartition.getPubSubTopic().isRealTime()) {
        return dedicatedConsumerService;
      }
      return defaultConsumerService;
    }

    @Override
    public List<KafkaConsumerService> getConsumerServices() {
      return Arrays.asList(defaultConsumerService, dedicatedConsumerService);
    }
  }

  public class CurrentVersionConsumerPoolStrategy implements ConsumerPoolStrategy {
    private final KafkaConsumerService consumerServiceForCurrentVersionAAWCLeader;
    private final KafkaConsumerService consumerServiceForCurrentVersionNonAAWCLeader;
    private final KafkaConsumerService consumerServiceForNonCurrentVersionAAWCLeader;
    private final KafkaConsumerService consumerServiceNonCurrentNonAAWCLeader;

    List<KafkaConsumerService> consumerServices = new ArrayList<>();

    public CurrentVersionConsumerPoolStrategy() {
      this.consumerServiceForCurrentVersionAAWCLeader = consumerServiceConstructor
          .apply(serverConfig.getConsumerPoolSizeForCurrentVersionAAWCLeader(), "_for_current_aa_wc_leader");
      consumerServices.add(consumerServiceForCurrentVersionAAWCLeader);
      this.consumerServiceForCurrentVersionNonAAWCLeader = consumerServiceConstructor
          .apply(serverConfig.getConsumerPoolSizeForCurrentVersionNonAAWCLeader(), "_for_current_non_aa_wc_leader");
      consumerServices.add(consumerServiceForCurrentVersionNonAAWCLeader);
      this.consumerServiceForNonCurrentVersionAAWCLeader = consumerServiceConstructor
          .apply(serverConfig.getConsumerPoolSizeForNonCurrentVersionAAWCLeader(), "_for_non_current_aa_wc_leader");
      consumerServices.add(consumerServiceForNonCurrentVersionAAWCLeader);
      this.consumerServiceNonCurrentNonAAWCLeader = consumerServiceConstructor.apply(
          serverConfig.getConsumerPoolSizeForNonCurrentVersionNonAAWCLeader(),
          "_for_non_current_non_aa_wc_leader");
      consumerServices.add(consumerServiceNonCurrentNonAAWCLeader);
    }

    @Override
    public KafkaConsumerService delegateKafkaConsumerServiceFor(
        PubSubTopic versionTopic,
        PartitionReplicaIngestionContext topicPartitionReplicaRole) {
      PartitionReplicaIngestionContext.VersionRole versionRole = topicPartitionReplicaRole.getVersionRole();
      PartitionReplicaIngestionContext.WorkloadType workloadType = topicPartitionReplicaRole.getWorkloadType();
      PubSubTopicPartition pubSubTopicPartition = topicPartitionReplicaRole.getPubSubTopicPartition();
      PubSubTopic pubSubTopic = pubSubTopicPartition.getPubSubTopic();
      if (versionRole.equals(PartitionReplicaIngestionContext.VersionRole.CURRENT)) {
        if (workloadType.equals(PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE)
            && pubSubTopic.isRealTime()) {
          return consumerServiceForCurrentVersionAAWCLeader;
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
    DEFAULT, AA_OR_WC_LEADER_DEDICATED, CURRENT_VERSION_PRIORITIZATION;
  }
}
