package com.linkedin.venice.listener;

import static com.linkedin.venice.response.VeniceReadResponseStatus.BAD_REQUEST;
import static com.linkedin.venice.response.VeniceReadResponseStatus.SERVICE_UNAVAILABLE;
import static com.linkedin.venice.response.VeniceReadResponseStatus.TOO_MANY_REQUESTS;
import static com.linkedin.venice.throttle.EventThrottler.REJECT_STRATEGY;

import com.linkedin.davinci.config.VeniceServerConfig;
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
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.throttle.VeniceRateLimiter.RateLimiterType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
    implements RoutingDataRepository.RoutingDataChangedListener, StoreDataChangedListener, QuotaEnforcementHandler {
  private static final Logger LOGGER = LogManager.getLogger(ReadQuotaEnforcementHandler.class);
  public static final String SERVER_OVER_CAPACITY_MSG = "Server over capacity";
  public static final String INVALID_REQUEST_RESOURCE_MSG = "Invalid request resource: ";

  private final ConcurrentMap<String, VeniceRateLimiter> storeVersionRateLimiters = new VeniceConcurrentHashMap<>();
  private final ReadOnlyStoreRepository storeRepository;
  private final String thisNodeId;
  private final AggServerQuotaUsageStats stats;
  private final Clock clock;
  private final int quotaEnforcementIntervalInMs;
  private final int enforcementCapacityMultiple; // Token bucket capacity is refill amount times this multiplier
  private final RateLimiterType storeVersionRateLimiterType;

  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private volatile boolean initializedVolatile = false;
  private boolean initialized = false;
  private VeniceRateLimiter storageNodeRateLimiter;

  public ReadQuotaEnforcementHandler(
      VeniceServerConfig serverConfig,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      MetricsRepository metricsRepository) {
    this(serverConfig, storeRepository, customizedViewRepository, nodeId, stats, metricsRepository, Clock.systemUTC());
  }

  public ReadQuotaEnforcementHandler(
      VeniceServerConfig serverConfig,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      MetricsRepository metricsRepository,
      Clock clock) {
    this.quotaEnforcementIntervalInMs = serverConfig.getQuotaEnforcementIntervalInMs();
    this.enforcementCapacityMultiple = serverConfig.getQuotaEnforcementCapacityMultiple();
    this.storeVersionRateLimiterType = serverConfig.getStoreVersionQpsRateLimiterType();
    this.clock = clock;
    this.thisNodeId = nodeId;
    this.storageNodeRateLimiter = getRateLimiter(
        "StorageNodeQuota_" + nodeId,
        serverConfig.getNodeCapacityInRcu(),
        1,
        null,
        serverConfig.getStorageNodeRateLimiterType(),
        quotaEnforcementIntervalInMs,
        enforcementCapacityMultiple,
        clock);
    this.storeRepository = storeRepository;
    this.stats = stats;
    customizedViewRepository.thenAccept(cv -> {
      LOGGER.info("Initializing ReadQuotaEnforcementHandler with completed RoutingDataRepository");
      this.customizedViewRepository = cv;
      init();
    });
    LOGGER.info(
        "Rate limiter algorithms - storageNodeRateLimiter: {}, storeVersionRateLimiterType: {}",
        serverConfig.getStorageNodeRateLimiterType(),
        storeVersionRateLimiterType);
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

  /**
   * Enforce quota for a given request.  This is common to both HTTP and GRPC handlers. Respective handlers will
   * take actions such as retaining the request and passing it to the next handler, or sending an error response.
   * @param request RouterRequest
   * @return QuotaEnforcementResult
   */
  public QuotaEnforcementHandler.QuotaEnforcementResult enforceQuota(RouterRequest request) {
    String storeName = request.getStoreName();
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      return QuotaEnforcementHandler.QuotaEnforcementResult.BAD_REQUEST;
    }

    /*
     * If we haven't completed initialization or store does not have SN read quota enabled, allow all requests
     */
    if (!isInitialized() || !store.isStorageNodeReadQuotaEnabled()) {
      return QuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED;
    }

    int readCapacityUnits = getRcu(request);

    /*
     * First check per store version level quota; don't throttle retried request at store version level
     */
    VeniceRateLimiter veniceRateLimiter = storeVersionRateLimiters.get(request.getResourceName());
    if (veniceRateLimiter != null) {
      if (!request.isRetryRequest() && !veniceRateLimiter.tryAcquirePermit(readCapacityUnits)) {
        stats.recordRejected(request.getStoreName(), readCapacityUnits);
        return QuotaEnforcementHandler.QuotaEnforcementResult.REJECTED;
      }
    } else {
      // If this happens it is probably due to a short-lived race condition where the resource is being accessed before
      // the bucket is allocated. The request will be allowed based on node/server capacity so emit metrics accordingly.
      stats.recordAllowedUnintentionally(storeName, readCapacityUnits);
    }

    /*
     * Once we know store level quota has capacity, check node level capacity;
     * retried requests need to be throttled at node capacity level
     */
    if (!storageNodeRateLimiter.tryAcquirePermit(readCapacityUnits)) {
      return QuotaEnforcementHandler.QuotaEnforcementResult.OVER_CAPACITY;
    }

    stats.recordAllowed(storeName, readCapacityUnits);
    return QuotaEnforcementHandler.QuotaEnforcementResult.ALLOWED;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RouterRequest request) {
    QuotaEnforcementHandler.QuotaEnforcementResult result = enforceQuota(request);

    if (result == QuotaEnforcementHandler.QuotaEnforcementResult.BAD_REQUEST) {
      ctx.writeAndFlush(
          new HttpShortcutResponse(
              INVALID_REQUEST_RESOURCE_MSG + request.getResourceName(),
              BAD_REQUEST.getHttpResponseStatus()));
      return;
    }

    if (result == QuotaEnforcementHandler.QuotaEnforcementResult.REJECTED) {
      ctx.writeAndFlush(new HttpShortcutResponse(TOO_MANY_REQUESTS.getHttpResponseStatus()));
      return;
    }

    if (result == QuotaEnforcementHandler.QuotaEnforcementResult.OVER_CAPACITY) {
      ctx.writeAndFlush(
          new HttpShortcutResponse(SERVER_OVER_CAPACITY_MSG, SERVICE_UNAVAILABLE.getHttpResponseStatus()));
      return;
    }

    // If we reach here, the request is allowed; retain the request and pass it to the next handler
    ReferenceCountUtil.retain(request);
    ctx.fireChannelRead(request);
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
      storeVersionRateLimiters.remove(topic);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    long quotaInRcu = storeRepository.getStore(storeName).getReadQuotaInCU();
    storeVersionRateLimiters.compute(topic, (k, v) -> {
      VeniceRateLimiter rateLimiter = getRateLimiter(
          topic,
          quotaInRcu,
          thisNodeQuotaResponsibility,
          v,
          storeVersionRateLimiterType,
          quotaEnforcementIntervalInMs,
          enforcementCapacityMultiple,
          clock);

      if (rateLimiter != v) {
        stats.setNodeQuotaResponsibility(storeName, (long) Math.ceil(quotaInRcu * thisNodeQuotaResponsibility));
      }
      return rateLimiter;
    });
  }

  /**
   * Get the rate limiter for the store version. If the rate limiter is already created and the quota is the same,
   * return the existing rate limiter.
   */
  static VeniceRateLimiter getRateLimiter(
      String storeVersionName,
      long quotaInRcu,
      double thisNodeQuotaResponsibility,
      VeniceRateLimiter currentRateLimiter,
      RateLimiterType rateLimiterType,
      int quotaEnforcementIntervalInMs,
      int enforcementCapacityMultiple,
      Clock clock) {
    long newQuota = (long) Math.ceil(quotaInRcu * thisNodeQuotaResponsibility);
    // If the rate limiter is already created and the quota is the same, return the existing rate limiter
    if (currentRateLimiter != null && currentRateLimiter.getQuota() == newQuota) {
      return currentRateLimiter;
    }

    VeniceRateLimiter newRateLimiter;
    if (rateLimiterType == RateLimiterType.EVENT_THROTTLER_WITH_SILENT_REJECTION) {
      newRateLimiter =
          new EventThrottler(newQuota, quotaEnforcementIntervalInMs, storeVersionName, true, REJECT_STRATEGY);
    } else if (rateLimiterType == RateLimiterType.GUAVA_RATE_LIMITER) {
      newRateLimiter = new GuavaRateLimiter(newQuota);
    } else {
      newRateLimiter = TokenBucket.tokenBucketFromRcuPerSecond(
          quotaInRcu,
          thisNodeQuotaResponsibility,
          quotaEnforcementIntervalInMs,
          enforcementCapacityMultiple,
          clock);
    }
    newRateLimiter.setQuota(newQuota);
    return newRateLimiter;
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    // Ignore this event
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    storeVersionRateLimiters.remove(kafkaTopic);
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
    return storeVersionRateLimiters.keySet()
        .stream()
        .filter((topic) -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .collect(Collectors.toSet());
  }

  private void removeTopics(Set<String> topicsToRemove) {
    for (String topic: topicsToRemove) {
      customizedViewRepository.unSubscribeRoutingDataChange(topic, this);
      storeVersionRateLimiters.remove(topic);
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
   * Helper methods for unit testing
   */
  protected Set<String> getActiveStoreVersions() {
    return storeVersionRateLimiters.keySet();
  }

  public ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }

  public AggServerQuotaUsageStats getStats() {
    return stats;
  }

  void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  void setInitializedVolatile(boolean initializedVolatile) {
    this.initializedVolatile = initializedVolatile;
  }

  VeniceRateLimiter getStoreVersionRateLimiter(String storeVersion) {
    return storeVersionRateLimiters.get(storeVersion);
  }

  void setStoreVersionRateLimiter(String storeVersion, VeniceRateLimiter rateLimiter) {
    storeVersionRateLimiters.put(storeVersion, rateLimiter);
  }

  void setStorageNodeRateLimiter(VeniceRateLimiter rateLimiter) {
    storageNodeRateLimiter = rateLimiter;
  }
}
