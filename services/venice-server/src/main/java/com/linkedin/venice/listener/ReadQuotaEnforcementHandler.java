package com.linkedin.venice.listener;

import static com.linkedin.venice.throttle.EventThrottler.REJECT_STRATEGY;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
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
import com.linkedin.venice.stats.ServerReadQuotaUsageStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.throttle.VeniceRateLimiter.RateLimiterType;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class ReadQuotaEnforcementHandler extends SimpleChannelInboundHandler<RouterRequest>
    implements RoutingDataRepository.RoutingDataChangedListener, StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(ReadQuotaEnforcementHandler.class);
  public static final String SERVER_OVER_CAPACITY_MSG = "Server over capacity";
  public static final String INVALID_REQUEST_RESOURCE_MSG = "Invalid request resource: ";
  private static final long WAIT_FOR_CUSTOMIZED_VIEW_TIMEOUT_SECONDS = 180;
  private static final String QUOTA_ENFORCEMENT_HANDLER_NAME = "ReadQuotaEnforcementHandler";
  private static final String NO_RESOURCE_FOUND_MSG = "VeniceNoHelixResourceException";
  private static final String EMPTY_PARTITION_ASSIGNMENT_MSG = "empty partition assignment";
  private final ConcurrentMap<String, VeniceRateLimiter> storeVersionRateLimiters = new VeniceConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> storeQuotaChangeMap = new VeniceConcurrentHashMap<>();
  private final ReadOnlyStoreRepository storeRepository;
  private final String thisNodeId;
  private final AggServerQuotaUsageStats stats;
  private final Clock clock;
  private final int quotaEnforcementIntervalInMs;
  private final int enforcementCapacityMultiple; // Token bucket capacity is refill amount times this multiplier
  private final RateLimiterType storeVersionRateLimiterType;
  private final boolean quotaInitializationFallbackEnabled;
  /**
   * Used for quota allocation when global view of the partition/replica assignment is not available.
   */
  private final StorageEngineRepository storageEngineRepository;

  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private volatile boolean initializedVolatile = false;
  private VeniceRateLimiter storageNodeRateLimiter;

  public ReadQuotaEnforcementHandler(
      VeniceServerConfig serverConfig,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      StorageEngineRepository storageEngineRepository,
      String nodeId,
      AggServerQuotaUsageStats stats) {
    this(
        serverConfig,
        storeRepository,
        customizedViewRepository,
        storageEngineRepository,
        nodeId,
        stats,
        Clock.systemUTC());
  }

  public ReadQuotaEnforcementHandler(
      VeniceServerConfig serverConfig,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      StorageEngineRepository storageEngineRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      Clock clock) {
    this.quotaEnforcementIntervalInMs = serverConfig.getQuotaEnforcementIntervalInMs();
    this.enforcementCapacityMultiple = serverConfig.getQuotaEnforcementCapacityMultiple();
    this.storeVersionRateLimiterType = serverConfig.getStoreVersionQpsRateLimiterType();
    this.quotaInitializationFallbackEnabled = serverConfig.isReadQuotaInitializationFallbackEnabled();
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
    this.storageEngineRepository = storageEngineRepository;
    this.stats = stats;
    ExecutorService asyncInitExecutor =
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("server-read-quota-init-thread"));
    asyncInitExecutor.submit(() -> {
      try {
        this.customizedViewRepository =
            customizedViewRepository.get(WAIT_FOR_CUSTOMIZED_VIEW_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        LOGGER.info("Initializing {} with completed RoutingDataRepository", QUOTA_ENFORCEMENT_HANDLER_NAME);
        init();
        LOGGER.info("{} initialization completed", QUOTA_ENFORCEMENT_HANDLER_NAME);
      } catch (TimeoutException e) {
        LOGGER.error(
            "Unable to initialize {}, timed out while waiting for customized view repository",
            ReadQuotaEnforcementHandler.class.getSimpleName());
      } catch (Exception e) {
        LOGGER.error("Failed to initialize {}", QUOTA_ENFORCEMENT_HANDLER_NAME, e);
      }
    });
    asyncInitExecutor.shutdown();
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
      try {
        List<Version> versions = store.getVersions();
        for (Version version: versions) {
          customizedViewRepository.subscribeRoutingDataChange(version.kafkaTopicName(), this);
        }
        // also invoke handle store change to ensure corresponding token bucket and stats are initialized.
        handleStoreChanged(store);
      } catch (Exception e) {
        // Log the exception but continue to initialize quota for other stores in the cluster
        LOGGER.error(
            "Failed to initialize quota for store: {}. Will continue to initialize quota for other stores",
            store.getName(),
            e);
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
    return initializedVolatile;
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
    if (!store.isStorageNodeReadQuotaEnabled()) {
      return QuotaEnforcementResult.ALLOWED;
    }

    int readCapacityUnits = getRcu(request);
    if (!isInitialized()) {
      stats.recordAllowedUnintentionally(storeName, readCapacityUnits);
      return QuotaEnforcementResult.ALLOWED;
    }

    /*
     * First check per store version level quota; don't throttle retried request at store version level
     */
    VeniceRateLimiter veniceRateLimiter = storeVersionRateLimiters.get(request.getResourceName());
    int version = Version.parseVersionFromKafkaTopicName(request.getResourceName());
    if (veniceRateLimiter != null) {
      if (!request.isRetryRequest() && !veniceRateLimiter.tryAcquirePermit(readCapacityUnits)) {
        stats.recordRejected(request.getStoreName(), version, readCapacityUnits);
        return QuotaEnforcementResult.REJECTED;
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
      return QuotaEnforcementResult.OVER_CAPACITY;
    }

    stats.recordAllowed(storeName, version, readCapacityUnits);
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

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    // Ignore this event since the partition assignment triggered by EV won't contain the right states.
  }

  @Override
  public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
    updateQuota(partitionAssignment, false);
  }

  @Override
  public void onCustomizedViewAdded(PartitionAssignment partitionAssignment) {
    updateQuota(partitionAssignment, false);
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   * Then sums the quota portions allocated to this node and creates a TokenBucket for the resource.
   *
   * @param partitionAssignment Latest partitions assignments information including resource name and all instances
   *                            assigned with this resource.
   */
  private void updateQuota(PartitionAssignment partitionAssignment, boolean shouldLog) {
    String topic = partitionAssignment.getTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int version = Version.parseVersionFromKafkaTopicName(topic);
    Store store = storeRepository.getStore(storeName);
    if (partitionAssignment.getAllPartitions().isEmpty()) {
      handleQuotaUpdateWithoutPartitionAssignment(store, version, topic, EMPTY_PARTITION_ASSIGNMENT_MSG);
      return;
    }
    double thisNodeQuotaResponsibility = getNodeResponsibilityForQuota(partitionAssignment, thisNodeId);
    if (thisNodeQuotaResponsibility <= 0) {
      storeVersionRateLimiters.remove(topic);
      stats.getStoreStats(storeName).removeVersion(version);
      return;
    }
    long quotaInRcu = store.getReadQuotaInCU();
    computeNewRateLimiter(storeName, version, topic, quotaInRcu, thisNodeQuotaResponsibility, shouldLog);
  }

  private void computeNewRateLimiter(
      String storeName,
      int version,
      String topic,
      long quotaInRcu,
      double thisNodeQuotaResponsibility,
      boolean shouldLog) {
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
        stats
            .setNodeQuotaResponsibility(storeName, version, (long) Math.ceil(quotaInRcu * thisNodeQuotaResponsibility));
        if (shouldLog) {
          LOGGER.info(
              "New rate limiter calculated for store version: {} with total quota: {}, node responsibility: {}, instance quota: {}",
              topic,
              quotaInRcu,
              thisNodeQuotaResponsibility,
              rateLimiter.getQuota());
        }
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
    ServerReadQuotaUsageStats storeStats =
        stats.getNullableStoreStats(Version.parseStoreFromKafkaTopicName(kafkaTopic));
    if (storeStats != null) {
      storeStats.removeVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));
    }
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
    Long previousStoreReadQuota = storeQuotaChangeMap.get(store.getName());
    boolean shouldLog = previousStoreReadQuota != null && previousStoreReadQuota != store.getReadQuotaInCU();
    if (shouldLog) {
      LOGGER.info(
          "Store: {} read quota changed from {} to {}",
          store.getName(),
          previousStoreReadQuota,
          store.getReadQuotaInCU());
    }
    Set<String> toBeRemovedTopics = getStoreTopics(store.getName());
    List<String> topics =
        store.getVersions().stream().map((version) -> version.kafkaTopicName()).collect(Collectors.toList());
    int currentVersion = store.getCurrentVersion();
    int backupVersion = 0;
    for (String topic: topics) {
      toBeRemovedTopics.remove(topic);
      customizedViewRepository.subscribeRoutingDataChange(topic, this);
      int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
      try {
        /**
         * make sure we're up-to-date after registering as a listener
         *
         * During a new push, a new version is added to the version list of the Store metadata before the push actually
         * starts, so this function (ReadQuotaEnforcementHandler#handleStoreChanged()) is invoked before the new
         * resource assignment shows up in the external view, so calling customizedViewRepository.getPartitionAssignments() for
         * a future version will fail in most cases, because the new topic is not in the external view at all.
         *
         */
        updateQuota(customizedViewRepository.getPartitionAssignments(topic), shouldLog);
        if (versionNumber != currentVersion && versionNumber > backupVersion
            && VersionStatus.isBootstrapCompleted(store.getVersionStatus(versionNumber))) {
          backupVersion = versionNumber;
        }
      } catch (VeniceNoHelixResourceException e) {
        /**
         * Store metadata (version info) is maintained separately from EV/CV so there could be many ways a race could
         * occur. e.g. the version is deleted and CV/EV reflected the change faster than store metadata.
         * Alternatively, EV/CV could also be lagging behind about a new version creation or after host restart.
         */
        handleQuotaUpdateWithoutPartitionAssignment(store, versionNumber, topic, NO_RESOURCE_FOUND_MSG);
      }
    }
    removeTopics(toBeRemovedTopics);
    if (currentVersion > 0) {
      stats.setCurrentVersion(store.getName(), currentVersion);
    }
    if (backupVersion > 0) {
      stats.setBackupVersion(store.getName(), backupVersion);
    }
    storeQuotaChangeMap.put(store.getName(), store.getReadQuotaInCU());
  }

  /**
   * If partition assignment is not available for any reason(s) instead of failing quota initialization we will go ahead
   * and update the quota based on local storage state. Leveraging {@link StorageEngineRepository} partition assignment
   * information and assuming this replica will be responsible for all traffic for assigned partition(s).
   */
  private void handleQuotaUpdateWithoutPartitionAssignment(
      Store store,
      int versionNumber,
      String topic,
      String reason) {
    Version version = store.getVersion(versionNumber);
    if (version != null && version.getStatus().equals(VersionStatus.ONLINE)) {
      if (quotaInitializationFallbackEnabled) {
        LOGGER.warn(
            "Unable to get partition assignment due to: {} for resource: {}, Will try to use fallback strategy to initialize the rate limiter",
            reason,
            topic);
        long quotaInRcu = store.getReadQuotaInCU();
        double thisNodeQuotaResponsibility = 0;
        StorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(topic);
        if (storageEngine != null) {
          double assignedPartitionCount = storageEngine.getPartitionIds().size();
          thisNodeQuotaResponsibility = assignedPartitionCount / (double) version.getPartitionCount();
        }
        LOGGER.info(
            "Read quota fallback strategy calculated node responsibility of: {} for resource: {}",
            thisNodeQuotaResponsibility,
            topic);
        if (thisNodeQuotaResponsibility <= 0) {
          storeVersionRateLimiters.remove(topic);
          stats.getStoreStats(store.getName()).removeVersion(versionNumber);
        } else {
          computeNewRateLimiter(store.getName(), versionNumber, topic, quotaInRcu, thisNodeQuotaResponsibility, true);
        }
      } else {
        LOGGER.error("Unable to get partition assignment due to: {} for resource: {}, Will fail open", reason, topic);
      }
    }
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
      ServerReadQuotaUsageStats storeStats = stats.getNullableStoreStats(Version.parseStoreFromKafkaTopicName(topic));
      if (storeStats != null) {
        storeStats.removeVersion(Version.parseVersionFromKafkaTopicName(topic));
      }
    }
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

  // For unit testing only
  void setInitializedVolatile(boolean initializedVolatile) {
    this.initializedVolatile = initializedVolatile;
  }

  // For unit testing only
  VeniceRateLimiter getStoreVersionRateLimiter(String storeVersion) {
    return storeVersionRateLimiters.get(storeVersion);
  }

  // For unit testing only
  void setStoreVersionRateLimiter(String storeVersion, VeniceRateLimiter rateLimiter) {
    storeVersionRateLimiters.put(storeVersion, rateLimiter);
  }

  // For unit testing only
  void setStorageNodeRateLimiter(VeniceRateLimiter rateLimiter) {
    storageNodeRateLimiter = rateLimiter;
  }
}
