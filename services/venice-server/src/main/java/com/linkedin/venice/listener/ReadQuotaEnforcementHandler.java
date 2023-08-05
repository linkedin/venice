package com.linkedin.venice.listener;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.grpc.GrpcErrorCodes;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.VeniceGrpcHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.utils.ExpiringSet;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class ReadQuotaEnforcementHandler extends SimpleChannelInboundHandler<RouterRequest>
    implements RoutingDataRepository.RoutingDataChangedListener, StoreDataChangedListener, VeniceGrpcHandler {
  private static final Logger LOGGER = LogManager.getLogger(ReadQuotaEnforcementHandler.class);
  private final ConcurrentMap<String, TokenBucket> storeVersionBuckets = new VeniceConcurrentHashMap<>();
  private final TokenBucket storageNodeBucket;
  private final ReadOnlyStoreRepository storeRepository;
  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private final String thisNodeId;
  private final AggServerQuotaUsageStats stats;
  private final Clock clock;
  private boolean enforcing = true;
  private final ExpiringSet<String> noBucketStores = new ExpiringSet<>(30, TimeUnit.SECONDS);

  private volatile boolean initializedVolatile = false;
  private boolean initialized = false;

  // TODO make these configurable
  private final int enforcementIntervalSeconds = 10; // TokenBucket refill interval
  private final int enforcementCapacityMultiple = 5; // Token bucket capacity is refill amount times this multiplier

  public ReadQuotaEnforcementHandler(
      long storageNodeRcuCapacity,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats) {
    this(storageNodeRcuCapacity, storeRepository, customizedViewRepository, nodeId, stats, Clock.systemUTC());
  }

  public ReadQuotaEnforcementHandler(
      long storageNodeRcuCapacity,
      ReadOnlyStoreRepository storeRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      String nodeId,
      AggServerQuotaUsageStats stats,
      Clock clock) {
    this.clock = clock;
    this.storageNodeBucket = tokenBucketfromRcuPerSecond(storageNodeRcuCapacity, 1);
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
   * Initialize token buckets for all resources in the customized view repository.
   */
  public final void init() {
    storeRepository.registerStoreDataChangedListener(this);
    ResourceAssignment resourceAssignment = customizedViewRepository.getResourceAssignment();
    if (resourceAssignment == null) {
      LOGGER.error(
          "Null resource assignment from HelixCustomizedViewOfflinePushRepository in ReadQuotaEnforcementHandler");
    } else {
      for (String resource: customizedViewRepository.getResourceAssignment().getAssignedResources()) {
        this.onExternalViewChange(customizedViewRepository.getPartitionAssignments(resource));
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

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RouterRequest request) {
    String storeName = request.getStoreName();
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      ctx.writeAndFlush(
          new HttpShortcutResponse(
              "Invalid request resource " + request.getResourceName(),
              HttpResponseStatus.BAD_REQUEST));
      return;
    }
    if (!isInitialized() || !store.isStorageNodeReadQuotaEnabled()) {
      // If we haven't completed initialization or store does not have SN read quota enabled, allow all requests
      // Note: not recording any metrics. Lack of metrics indicates an issue with initialization
      ReferenceCountUtil.retain(request);
      ctx.fireChannelRead(request);
      return;
    }
    int rcu = getRcu(request); // read capacity units

    /**
     * First check store bucket for capacity; don't throttle retried request at store version level
     */
    TokenBucket tokenBucket = storeVersionBuckets.get(request.getResourceName());
    if (tokenBucket != null && !request.isRetryRequest()) {
      if (!tokenBucket.tryConsume(rcu)) {
        // TODO: check if extra node capacity and can still process this request out of quota
        stats.recordRejected(storeName, rcu);
        if (enforcing) {
          long storeQuota = store.getReadQuotaInCU();
          float thisNodeRcuPerSecond = storeVersionBuckets.get(request.getResourceName()).getAmortizedRefillPerSecond();
          String errorMessage =
              "Total quota for store " + storeName + " is " + storeQuota + " RCU per second. Storage Node " + thisNodeId
                  + " is allocated " + thisNodeRcuPerSecond + " RCU per second which has been exceeded.";
          ctx.writeAndFlush(new HttpShortcutResponse(errorMessage, HttpResponseStatus.TOO_MANY_REQUESTS));
          return;
        }
      }
    } else if (enforcing && !noBucketStores.contains(request.getResourceName())) {
      // If this happens it is probably due to a short-lived race condition
      // of the resource being allocated before the bucket is allocated.
      LOGGER.warn(
          "Request for resource: {} but no TokenBucket for that resource. Not yet enforcing quota",
          request.getResourceName());
      // TODO: We could consider initializing a bucket. Would need to carefully consider this case.
      noBucketStores.add(request.getResourceName()); // So that we only log this once every 30 seconds
    }

    /**
     * Once we know store bucket has capacity, check node bucket for capacity;
     * retried requests need to be throttled at node capacity level
     */
    if (!storageNodeBucket.tryConsume(rcu)) {
      stats.recordRejected(storeName, rcu);
      if (enforcing) {
        ctx.writeAndFlush(new HttpShortcutResponse("Server over capacity", HttpResponseStatus.SERVICE_UNAVAILABLE));
        return;
      }
    }

    ReferenceCountUtil.retain(request);
    ctx.fireChannelRead(request);
    stats.recordAllowed(storeName, rcu);
    stats.recordReadQuotaUsage(storeName, storageNodeBucket.getStaleUsageRatio());
  }

  @Override
  public void grpcRead(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    RouterRequest request = ctx.getRouterRequest();
    String storeName = request.getStoreName();
    Store store = storeRepository.getStore(storeName);

    if (store == null) {
      String error = "Invalid request resource " + request.getResourceName();
      LOGGER.error(error);
      ctx.setError();
      ctx.getVeniceServerResponseBuilder().setErrorCode(GrpcErrorCodes.BAD_REQUEST).setErrorMessage(error);
      pipeline.processRequest(ctx);
      return;
    }

    if (!isInitialized() || !store.isStorageNodeReadQuotaEnabled()) {
      ReferenceCountUtil.retain(request);
      // no quota limit is enforced
      pipeline.processRequest(ctx);
      return;
    }

    int rcu = getRcu(request); // read capacity units

    TokenBucket tokenBucket = storeVersionBuckets.get(request.getResourceName());
    if (tokenBucket != null && !request.isRetryRequest()) {
      if (!tokenBucket.tryConsume(rcu)) {
        stats.recordRejected(storeName, rcu);

        if (enforcing) {
          long storeQuota = store.getReadQuotaInCU();
          float thisNodeRcuPerSecond = storeVersionBuckets.get(request.getResourceName()).getAmortizedRefillPerSecond();

          String errorMessage =
              "Total quota for store " + storeName + " is " + storeQuota + " RCU per second. Storage Node " + thisNodeId
                  + " is allocated " + thisNodeRcuPerSecond + " RCU per second which has been exceeded.";

          LOGGER.error(errorMessage);
          ctx.setError();
          ctx.getVeniceServerResponseBuilder()
              .setErrorCode(GrpcErrorCodes.TOO_MANY_REQUESTS)
              .setErrorMessage(errorMessage);
          pipeline.processRequest(ctx);
          return;
        }
      }
    } else if (enforcing && !noBucketStores.contains(request.getResourceName())) {
      LOGGER.warn(
          "Request for resource: {} but no TokenBucket for that resource. Not yet enforcing quota",
          request.getResourceName());

      noBucketStores.add(request.getResourceName());
    }

    if (!storageNodeBucket.tryConsume(rcu)) {
      stats.recordRejected(storeName, rcu);
      if (enforcing) {
        LOGGER.error("Server over capacity when performing gRPC request");
        String errorMessage = "Server over capacity";
        ctx.setError();
        ctx.getVeniceServerResponseBuilder()
            .setErrorCode(GrpcErrorCodes.SERVICE_UNAVAILABLE)
            .setErrorMessage(errorMessage);
        pipeline.processRequest(ctx);
        return;
      }
    }

    ReferenceCountUtil.retain(request);
    stats.recordAllowed(storeName, rcu);
    stats.recordReadQuotaUsage(storeName, storageNodeBucket.getStaleUsageRatio());

    pipeline.processRequest(ctx);
  }

  @Override
  public void grpcWrite(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    pipeline.processResponse(ctx);
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
        return request.getKeyCount();
      case COMPUTE:
        // Eventually, we'll want to add some extra cost beyond the look up cost for compute operations.
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
    long thisRefillAmount = (long) Math.ceil(totalRefillAmount * thisBucketProportionOfTotalRcu);
    long thisCapacity = (long) Math.ceil(totalCapacity * thisBucketProportionOfTotalRcu);
    return new TokenBucket(thisCapacity, thisRefillAmount, enforcementIntervalSeconds, SECONDS, clock);
  }

  @Override
  public void onExternalViewChange(PartitionAssignment partitionAssignment) {
    updateQuota(partitionAssignment);
  }

  @Override
  public void onCustomizedViewChange(PartitionAssignment partitionAssignment) {
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
    double thisNodeQuotaResponsibility =
        getNodeResponsibilityForQuota(customizedViewRepository, partitionAssignment, thisNodeId);
    if (thisNodeQuotaResponsibility <= 0) {
      LOGGER.warn(
          "Routing data changed on quota enforcement handler with 0 replicas assigned to this node, removing quota for resource: {}",
          topic);
      storeVersionBuckets.remove(topic);
      return;
    }
    long quotaInRcu = storeRepository.getStore(Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic()))
        .getReadQuotaInCU();
    TokenBucket newStoreBucket = tokenBucketfromRcuPerSecond(quotaInRcu, thisNodeQuotaResponsibility);
    storeVersionBuckets.put(topic, newStoreBucket); // put is atomic, so this method is thread-safe
  }

  @Override
  public void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    // Ignore this event
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   *
   * @param partitionAssignment
   * @param nodeId
   * @return
   */
  protected static double getNodeResponsibilityForQuota(
      HelixCustomizedViewOfflinePushRepository customizedViewRepository,
      PartitionAssignment partitionAssignment,
      String nodeId) {
    double thisNodePartitionPortion = 0; // 1 means full responsibility for serving a partition. 0.33 means serving 1 of
    // 3 replicas for a partition
    for (Partition p: partitionAssignment.getAllPartitions()) {
      List<String> readyToServeInstances = new ArrayList<>();
      for (ReplicaState replicaState: customizedViewRepository
          .getReplicaStates(partitionAssignment.getTopic(), p.getId())) {
        if (replicaState.isReadyToServe()) {
          readyToServeInstances.add(replicaState.getParticipantId());
        }
      }
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

  @Override
  public void handleStoreChanged(Store store) {
    Set<String> oldTopics = getStoreTopics(store.getName());

    List<String> topics =
        store.getVersions().stream().map((version) -> version.kafkaTopicName()).collect(Collectors.toList());
    for (String topic: topics) {
      int topicVersion = Version.parseVersionFromKafkaTopicName(topic);
      // No need to subscribe StorageQuotaHandler on versions other than current version.
      if (store.getCurrentVersion() != topicVersion) {
        continue;
      }
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
        this.onExternalViewChange(customizedViewRepository.getPartitionAssignments(topic));
      } catch (VeniceNoHelixResourceException e) {
        Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(topic));
        if (version.isPresent() && version.get().getStatus().equals(VersionStatus.ONLINE)) {
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
           * oldest version that should be retire, they should exist on external view if they are online.
           */
          if (!isLatestVersion(Version.parseVersionFromKafkaTopicName(topic), topics)) {
            throw new VeniceException(
                "Metadata for store " + store.getName() + " shows that version " + version.get().getNumber()
                    + " is online but couldn't find the resource in external view:",
                e);
          }
        }
      }
      oldTopics.remove(topic);
    }

    removeTopics(oldTopics);
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
      int currentVersion = storeRepository.getStore(storeName).getCurrentVersion();
      String topic = Version.composeKafkaTopic(storeName, currentVersion);
      return storeVersionBuckets.get(topic);
    }
  }

  /**
   * The quota enforcer normally enforces quota, disable enforcement to have a "Dry-run" mode
   */
  public void disableEnforcement() {
    this.enforcing = false;
  }
}
