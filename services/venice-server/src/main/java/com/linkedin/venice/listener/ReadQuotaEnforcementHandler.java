package com.linkedin.venice.listener;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.stats.ServerQuotaTokenBucketStats;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class ReadQuotaEnforcementHandler extends SimpleChannelInboundHandler<RouterRequest>
    implements RoutingDataRepository.RoutingDataChangedListener, StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(ReadQuotaEnforcementHandler.class);
  private static final String SERVER_BUCKET_STATS_NAME = "venice-storage-node-token-bucket";
  public static final String SERVER_OVER_CAPACITY_MSG = "Server over capacity";
  public static final String INVALID_REQUEST_RESOURCE_MSG = "Invalid request resource: ";

  private final ConcurrentMap<String, TokenBucket> storeVersionBuckets = new VeniceConcurrentHashMap<>();
  private final TokenBucket storageNodeBucket;
  private final ServerQuotaTokenBucketStats storageNodeTokenBucketStats;
  private final ReadOnlyStoreRepository storeRepository;
  private final String thisNodeId;
  private final AggServerQuotaUsageStats stats;
  private final Clock clock;
  // TODO make these configurable
  private final int enforcementIntervalSeconds = 10; // TokenBucket refill interval
  private final int enforcementCapacityMultiple = 5; // Token bucket capacity is refill amount times this multiplier

  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private volatile boolean initializedVolatile = false;
  private boolean initialized = false;

  public ReadQuotaEnforcementHandler(
      long storageNodeRcuCapacity,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      MetricsRepository metricsRepository) {
    this(
        storageNodeRcuCapacity,
        storeRepository,
        customizedViewRepository,
        nodeId,
        stats,
        metricsRepository,
        Clock.systemUTC());
  }

  public ReadQuotaEnforcementHandler(
      long storageNodeRcuCapacity,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      MetricsRepository metricsRepository,
      Clock clock) {
    this.clock = clock;
    this.storageNodeBucket = tokenBucketfromRcuPerSecond(storageNodeRcuCapacity, 1);
    this.storageNodeTokenBucketStats =
        new ServerQuotaTokenBucketStats(metricsRepository, SERVER_BUCKET_STATS_NAME, () -> storageNodeBucket);
    this.storeRepository = storeRepository;
    this.thisNodeId = nodeId;
    this.stats = stats;
    customizedViewRepository.thenAccept(cv -> {
      LOGGER.info("Initializing ReadQuotaEnforcementHandler with completed RoutingDataRepository");
      this.customizedViewRepository = cv;
      init();
    });
  }

  /**
   * Initially, this is the key count.  As we develop a more accurate capacity model, this method can be refined.
   * @param request
   * @return
   */
  public static int getRcu(RouterRequest request) {
    switch (request.getRequestType()) {
      case SINGLE_GET:
        return 1;
      case MULTI_GET:
      case MULTI_GET_STREAMING:
      case COMPUTE:
      case COMPUTE_STREAMING:
        // Eventually, we'll want to add some extra cost beyond the lookup cost for compute operations.
        return request.getKeyCount();
      default:
        LOGGER.error(
            "Unknown request type: {}, request for resource: {}",
            request.getRequestType(),
            request.getResourceName());
        return Integer.MAX_VALUE;
    }
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   *
   * @param partitionAssignment
   * @param nodeId
   * @return
   */
  protected static double getNodeResponsibilityForQuota(PartitionAssignment partitionAssignment, String nodeId) {
    double thisNodePartitionPortion = 0; // 1 means full responsibility for serving a partition. 0.33 means serving 1 of
    // 3 replicas for a partition
    for (Partition p: partitionAssignment.getAllPartitions()) {
      List<String> readyToServeInstances =
          p.getReadyToServeInstances().stream().map(Instance::getNodeId).collect(Collectors.toList());
      long thisPartitionReplicaCount = readyToServeInstances.size();
      long thisNodeReplicaCount = 0;
      for (String instanceId: readyToServeInstances) {
        if (instanceId.equals(nodeId)) {
          thisNodeReplicaCount += 1;
        }
      }
      if (thisPartitionReplicaCount > 0) {
        thisNodePartitionPortion += thisNodeReplicaCount / (double) thisPartitionReplicaCount;
      }
    }
    return thisNodePartitionPortion / partitionAssignment.getAllPartitions().size();
  }

  /**
   * Initialize token buckets for all resources in the customized view repository.
   */
  public final void init() {
    storeRepository.registerStoreDataChangedListener(this);
    // The customizedViewRepository right after server start might not have all the resources. Use the store
    // repository's versions to subscribe for routing data change and initialize the corresponding read quota token
    // bucket once the CVs are available.
    for (Store store: storeRepository.getAllStores()) {
      List<Version> versions = store.getVersions();
      for (Version version: versions) {
        customizedViewRepository.subscribeRoutingDataChange(version.kafkaTopicName(), this);
      }
    }
    this.initializedVolatile = true;
  }

  /**
   *  We only ever expect the initialized value to flip from false to true, but that initialization might happen in
   *  a different thread.  By only checking the volatile variable if it is false, we guarantee that we see the change
   *  as early as possible and also allow the thread to cache the value once it goes true.
   * @return
   */
  public boolean isInitialized() {
    if (initialized) {
      return true;
    }
    if (initializedVolatile) {
      initialized = true;
      return true;
    }
    return false;
  }

  public enum QuotaEnforcementResult {
    ALLOWED, // request is allowed
    REJECTED, // too many requests (store level quota enforcement)
    OVER_CAPACITY, // server over capacity (server level quota enforcement)
    BAD_REQUEST, // bad request
  }

  /**
   * Enforce quota for a given request.  This is common to both HTTP and GRPC handlers. Respective handlers will
   * take actions such as retaining the request and passing it to the next handler, or sending an error response.
   * @param request RouterRequest
   * @return QuotaEnforcementResult
   */
  public QuotaEnforcementResult enforceQuota(RouterRequest request) {
    String storeName = request.getStoreName();
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      return QuotaEnforcementResult.BAD_REQUEST;
    }

    /*
     * If we haven't completed initialization or store does not have SN read quota enabled, allow all requests
     */
    if (!isInitialized() || !store.isStorageNodeReadQuotaEnabled()) {
      return QuotaEnforcementResult.ALLOWED;
    }

    int readCapacityUnits = getRcu(request);

    /*
     * First check per store version bucket for capacity; don't throttle retried request at store version level
     */
    TokenBucket veniceRateLimiter = storeVersionBuckets.get(request.getResourceName());
    if (veniceRateLimiter != null) {
      if (!request.isRetryRequest() && !veniceRateLimiter.tryConsume(readCapacityUnits)) {
        stats.recordRejected(request.getStoreName(), readCapacityUnits);
        return QuotaEnforcementResult.REJECTED;
      }
    } else {
      // If this happens it is probably due to a short-lived race condition where the resource is being accessed before
      // the bucket is allocated. The request will be allowed based on node/server capacity so emit metrics accordingly.
      stats.recordAllowedUnintentionally(storeName, readCapacityUnits);
    }

    /*
     * Once we know store bucket has capacity, check node bucket for capacity;
     * retried requests need to be throttled at node capacity level
     */
    if (!storageNodeBucket.tryConsume(readCapacityUnits)) {
      return QuotaEnforcementResult.OVER_CAPACITY;
    }

    stats.recordAllowed(storeName, readCapacityUnits);
    return QuotaEnforcementResult.ALLOWED;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RouterRequest request) {
    QuotaEnforcementResult result = enforceQuota(request);

    if (result == QuotaEnforcementResult.BAD_REQUEST) {
      ctx.writeAndFlush(
          new HttpShortcutResponse(
              INVALID_REQUEST_RESOURCE_MSG + request.getResourceName(),
              HttpResponseStatus.BAD_REQUEST));
      return;
    }

    if (result == QuotaEnforcementResult.REJECTED) {
      ctx.writeAndFlush(new HttpShortcutResponse(HttpResponseStatus.TOO_MANY_REQUESTS));
      return;
    }

    if (result == QuotaEnforcementResult.OVER_CAPACITY) {
      ctx.writeAndFlush(new HttpShortcutResponse(SERVER_OVER_CAPACITY_MSG, HttpResponseStatus.SERVICE_UNAVAILABLE));
      return;
    }

    // If we reach here, the request is allowed; retain the request and pass it to the next handler
    ReferenceCountUtil.retain(request);
    ctx.fireChannelRead(request);
  }

  /**
   * @see TokenBucket for an explanation of TokenBucket capacity, refill amount, and refill interval
   *
   * This method takes a rate in units per second, and scales it according to the desired refill interval, and the
   * multiple of refill we wish to use for capacity.  Since the resulting token bucket must have a whole number of
   * tokens in it's refill amount and capacity, if we only accept one parameter in this method (localRcuPerSecond) then
   * we might have suboptimal results given integer division.  For example, with a global rate of 1 Rcu per second, and
   * a proportion of 0.1, along with an enforcement interval of 10 seconds we would be forced to pass 1 localRcuPerSecond
   * which is 10x the desired quota.  By passing both the global rate of 1 Rcu per second and a desired proportion, this
   * method can multiply by the enforcement interval first before shrinking by the proportion, allowing the Bucket to
   * be configured with the correct 1 refill every 10 seconds.
   *
   * @param totalRcuPerSecond  Number of units per second to allow
   * @param thisBucketProportionOfTotalRcu For maximum fidelity of calculations. If you need a smaller portion of the
   *                                       RCU to be applied then set this to an appropriate multiplier.  Otherwise set
   *                                       this to 1.
   * @return
   */
  private TokenBucket tokenBucketfromRcuPerSecond(long totalRcuPerSecond, double thisBucketProportionOfTotalRcu) {
    long totalRefillAmount = totalRcuPerSecond * enforcementIntervalSeconds;
    long totalCapacity = totalRefillAmount * enforcementCapacityMultiple;
    long thisRefillAmount = calculateRefillAmount(totalRcuPerSecond, thisBucketProportionOfTotalRcu);
    long thisCapacity = (long) Math.ceil(totalCapacity * thisBucketProportionOfTotalRcu);
    return new TokenBucket(thisCapacity, thisRefillAmount, enforcementIntervalSeconds, SECONDS, clock);
  }

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    // Ignore this event since the partition assignment triggered by EV won't contain the right states.
  }

  @Override
  public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
    updateQuota(partitionAssignment);
  }

  @Override
  public void onCustomizedViewAdded(PartitionAssignment partitionAssignment) {
    updateQuota(partitionAssignment);
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   * Then sums the quota portions allocated to this node and creates a TokenBucket for the resource.
   *
   * @param partitionAssignment Newest partitions assignments information including resource name and  all of instances assigned to this resource.
   */
  private void updateQuota(PartitionAssignment partitionAssignment) {
    String topic = partitionAssignment.getTopic();
    if (partitionAssignment.getAllPartitions().isEmpty()) {
      LOGGER.warn(
          "QuotaEnforcementHandler updated with an empty partition map for topic: {}. Skipping update process",
          topic);
      return;
    }
    double thisNodeQuotaResponsibility = getNodeResponsibilityForQuota(partitionAssignment, thisNodeId);
    if (thisNodeQuotaResponsibility <= 0) {
      LOGGER.warn(
          "Routing data changed on quota enforcement handler with 0 replicas assigned to this node, removing quota for resource: {}",
          topic);
      storeVersionBuckets.remove(topic);
      return;
    }
    long quotaInRcu = storeRepository.getStore(Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic()))
        .getReadQuotaInCU();
    storeVersionBuckets.compute(topic, (k, v) -> {
      long newRefillAmount = calculateRefillAmount(quotaInRcu, thisNodeQuotaResponsibility);
      if (v == null || v.getAmortizedRefillPerSecond() * enforcementIntervalSeconds != newRefillAmount) {
        // only replace the existing bucket if the difference is greater than 1
        return tokenBucketfromRcuPerSecond(quotaInRcu, thisNodeQuotaResponsibility);
      } else {
        return v;
      }
    });
    String storeName = Version.parseStoreFromVersionTopic(topic);
    stats.setStoreTokenBucket(storeName, getBucketForStore(storeName));
  }

  private long calculateRefillAmount(long totalRcuPerSecond, double thisBucketProportionOfTotalRcu) {
    long totalRefillAmount = totalRcuPerSecond * enforcementIntervalSeconds;
    return (long) Math.ceil(totalRefillAmount * thisBucketProportionOfTotalRcu);
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    // Ignore this event
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    storeVersionBuckets.remove(kafkaTopic);
  }

  @Override
  public void handleStoreCreated(Store store) {
    handleStoreChanged(store);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    Set<String> topics = getStoreTopics(storeName);
    removeTopics(topics);
  }

  /**
   * This is where we add new {@link TokenBucket} for new store version and remove irrelevant ones. We should keep the
   * same number of token buckets as the number of active versions. This is because readers like FC might be lagging
   * behind or vice versa. This way quota will still be enforced properly during the version swap or transition period.
   */
  @Override
  public void handleStoreChanged(Store store) {
    Set<String> toBeRemovedTopics = getStoreTopics(store.getName());

    List<String> topics =
        store.getVersions().stream().map((version) -> version.kafkaTopicName()).collect(Collectors.toList());
    for (String topic: topics) {
      toBeRemovedTopics.remove(topic);
      customizedViewRepository.subscribeRoutingDataChange(topic, this);
      try {
        /**
         * make sure we're up-to-date after registering as a listener
         *
         * During a new push, a new version is added to the version list of the Store metadata before the push actually
         * starts, so this function (ReadQuotaEnforcementHandler#handleStoreChanged()) is invoked before the new
         * resource assignment shows up in the external view, so calling customizedViewRepository.getPartitionAssignments() for
         * a the future version will fail in most cases, because the new topic is not in the external view at all.
         *
         */
        this.onCustomizedViewChange(customizedViewRepository.getPartitionAssignments(topic));
      } catch (VeniceNoHelixResourceException e) {
        Version version = store.getVersion(Version.parseVersionFromKafkaTopicName(topic));
        if (version != null && version.getStatus().equals(VersionStatus.ONLINE)) {
          /**
           * The store metadata believes this version is online, but the partition assignment is not in the
           * external view.
           */
          if (isOldestVersion(Version.parseVersionFromKafkaTopicName(topic), topics)) {
            /**
             * It could happen to a tiny store. When the push job completes, the status of the latest version will
             * be updated, which will result in invoking this handleStoreChanged() function; then the helix resource
             * of the old version will be dropped. Dropping the related helix resource completes too fast, even
             * before this store update callback completes, so it couldn't find the old resource in the external
             * view.
             */

            // do not remove this topic from the old topics set
            continue;
          }

          /**
           * For future version, it's possible that store metadata update callback is invoked faster than
           * the external view change callback; but for any other versions between future version and the
           * oldest version that should be retired, they should exist on external view if they are online.
           */
          if (!isLatestVersion(Version.parseVersionFromKafkaTopicName(topic), topics)) {
            throw new VeniceException(
                "Metadata for store " + store.getName() + " shows that version " + version.getNumber()
                    + " is online but couldn't find the resource in external view:",
                e);
          }
        }
      }
    }
    removeTopics(toBeRemovedTopics);
  }

  private Set<String> getStoreTopics(String storeName) {
    return storeVersionBuckets.keySet()
        .stream()
        .filter((topic) -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .collect(Collectors.toSet());
  }

  private void removeTopics(Set<String> topicsToRemove) {
    for (String topic: topicsToRemove) {
      customizedViewRepository.unSubscribeRoutingDataChange(topic, this);
      storeVersionBuckets.remove(topic);
    }
  }

  private boolean isOldestVersion(int version, List<String> topics) {
    for (String topic: topics) {
      if (Version.parseVersionFromKafkaTopicName(topic) < version) {
        // there is a smaller version
        return false;
      }
    }
    return true;
  }

  private boolean isLatestVersion(int version, List<String> topics) {
    for (String topic: topics) {
      if (Version.parseVersionFromKafkaTopicName(topic) > version) {
        // there is a bigger version
        return false;
      }
    }
    return true;
  }

  /**
   * For tests
   * @return
   */
  protected Set<String> listTopics() {
    return storeVersionBuckets.keySet();
  }

  public TokenBucket getBucketForStore(String storeName) {
    if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
      return storageNodeBucket;
    } else {
      Store store = storeRepository.getStore(storeName);
      if (store == null) {
        return null;
      }

      int currentVersion = store.getCurrentVersion();
      String topic = Version.composeKafkaTopic(storeName, currentVersion);
      return storeVersionBuckets.get(topic);
    }
  }

  public ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public ConcurrentMap<String, TokenBucket> getStoreVersionBuckets() {
    return storeVersionBuckets;
  }

  public boolean storageConsumeRcu(int rcu) {
    return !storageNodeBucket.tryConsume(rcu);
  }

  public AggServerQuotaUsageStats getStats() {
    return stats;
  }
}
