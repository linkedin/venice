package com.linkedin.venice.listener;

import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
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
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.throttle.TokenBucket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static java.util.concurrent.TimeUnit.*;


public class StorageQuotaEnforcementHandler extends SimpleChannelInboundHandler<RouterRequest> implements RoutingDataRepository.RoutingDataChangedListener, StoreDataChangedListener {

  private static final Logger logger = Logger.getLogger(StorageQuotaEnforcementHandler.class);
  private final ConcurrentMap<String, TokenBucket> storeVersionBuckets = new ConcurrentHashMap<>();
  private final TokenBucket storageNodeBucket;
  private final ReadOnlyStoreRepository storeRepository;
  private final RoutingDataRepository routingRepository;
  private final String thisNodeId;
  private final Clock clock;

  //TODO make these configurable
  private final int enforcementIntervalSeconds = 10; // TokenBucket refill interval
  private final int enforcementCapacityMultiple = 5; // Token bucket capacity is refill amount times this multiplier


  public StorageQuotaEnforcementHandler(long storageNodeRcuCapacity, ReadOnlyStoreRepository storeRepository, RoutingDataRepository routingRepository, String nodeId){
    this(storageNodeRcuCapacity, storeRepository, routingRepository, nodeId, Clock.systemUTC());
  }

  //TODO: How does storeVersionBuckets get initialized
  public  StorageQuotaEnforcementHandler(long storageNodeRcuCapacity, ReadOnlyStoreRepository storeRepository, RoutingDataRepository routingRepository, String nodeId, Clock clock){
    this.clock = clock;
    this.storageNodeBucket = tokenBucketfromRcuPerSecond(storageNodeRcuCapacity, 1);
    this.storeRepository = storeRepository;
    this.routingRepository = routingRepository;
    this.thisNodeId = nodeId;
    init();
  }

  /**
   * Initialize token buckets for all resources in the routingDataRepository
   */
  public void init(){
    ResourceAssignment resourceAssignment = routingRepository.getResourceAssignment();
    if (null == resourceAssignment) {
      logger.error("Null resource assignment from RoutingDataRepository in StorageQuotaEnforcementHandler");
      return;
    }
    for (String resource : routingRepository.getResourceAssignment().getAssignedResources()){
      this.onRoutingDataChanged(routingRepository.getPartitionAssignments(resource));
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RouterRequest request) {
    int rcu = getRcu(request); //read capacity units

    //First check store bucket for capacity
    if (storeVersionBuckets.containsKey(request.getResourceName())) {
      if (!storeVersionBuckets.get(request.getResourceName()).tryConsume(rcu)) {
        // TODO: check if extra node capacity and can still process this request out of quota
        String storeName = Version.parseStoreFromKafkaTopicName(request.getResourceName());
        long storeQuota = storeRepository.getStore(storeName).getReadQuotaInCU();
        float thisNodeRcuPerSecond = storeVersionBuckets.get(request.getResourceName()).getAmortizedRefillPerSecond();
        String errorMessage = "Total quota for store " + storeName + " is " + storeQuota + ", this node's portion is "
            + thisNodeRcuPerSecond + " which has been exceeded.";
        ctx.writeAndFlush(new HttpShortcutResponse(errorMessage, HttpResponseStatus.TOO_MANY_REQUESTS));
        ctx.close();
        return;
      }
    } else { // If this happens it is probably due to a short-lived race condition
      // of the resource being allocated before the bucket is allocated.
      logger.warn("Request for resource " + request.getResourceName() + " but no TokenBucket for that resource.  Not yet enforcing quota");
      //TODO: We could consider initializing a bucket.  Would need to carefully consider this case.
    }

    //Once we know store bucket has capacity, check node bucket for capacity
    if (!storageNodeBucket.tryConsume(rcu)) {
      ctx.writeAndFlush(new HttpShortcutResponse("Server over capacity", HttpResponseStatus.SERVICE_UNAVAILABLE));
      ctx.close();
      return;
    }

    ReferenceCountUtil.retain(request);
    ctx.fireChannelRead(request);
    return;
  }

  /**
   * Initially, this is the key count.  As we develop a more accurate capacity model, this method can be refined.
   * @param request
   * @return
   */
  private int getRcu(RouterRequest request){
    if (request.getRequestType().equals(RequestType.SINGLE_GET)){
      return 1;
    } else if (request.getRequestType().equals(RequestType.MULTI_GET)){
      return ((MultiGetRouterRequestWrapper) request).getKeyCount();
    } else {
      logger.error("Unknown request type " + request.getRequestType().toString()
          + ", request for resource: " + request.getResourceName());
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
  private TokenBucket tokenBucketfromRcuPerSecond(long totalRcuPerSecond, double thisBucketProportionOfTotalRcu){
    long totalRefillAmount = totalRcuPerSecond * enforcementIntervalSeconds;
    long totalCapacity = totalRefillAmount * enforcementCapacityMultiple;
    long thisRefillAmount = (long) Math.ceil(totalRefillAmount * thisBucketProportionOfTotalRcu);
    long thisCapacity = (long) Math.ceil(totalCapacity * thisBucketProportionOfTotalRcu);
    return new TokenBucket(thisCapacity, thisRefillAmount, enforcementIntervalSeconds, SECONDS, clock);
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   * Then sums the quota portions allocated to this node and creates a TokenBucket for the resource.
   *
   * @param partitionAssignment Newest partitions assignments information including resource name and  all of instances assigned to this resource.
   */
  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    String topic = partitionAssignment.getTopic();
    double thisNodeQuotaResponsibility = getNodeResponsibilityForQuota(partitionAssignment, thisNodeId);
    if (thisNodeQuotaResponsibility <= 0){
      logger.warn("Routing data changed on quota enforcement handler with 0 replicas assigned to this node, removing quota for resource: " + topic);
      storeVersionBuckets.remove(topic);
      return;
    }
    long quotaInRcu = storeRepository.getStore(
        Version.parseStoreFromKafkaTopicName(partitionAssignment.getTopic())
    ).getReadQuotaInCU();
    TokenBucket newStoreBucket = tokenBucketfromRcuPerSecond(
        quotaInRcu, thisNodeQuotaResponsibility);
    storeVersionBuckets.put(topic, newStoreBucket); //put is atomic, so this method is thread-safe
  }

  /**
   * Recalculates the amount of quota that this node should serve given the partition assignment.  Assumes each
   * partition gets an even portion of quota, and for each partition divides the quota by the readyToServe instances.
   *
   * @param partitionAssignment
   * @param nodeId
   * @return
   */
  protected static double getNodeResponsibilityForQuota(PartitionAssignment partitionAssignment, String nodeId){
    double thisNodePartitionPortion = 0; // 1 means full responsibility for serving a partition.  0.33 means serving 1 of 3 replicas for a partition
    for (Partition p : partitionAssignment.getAllPartitions()) {
      long thisPartitionReplicaCount = p.getReadyToServeInstances().size();
      long thisNodeReplicaCount = 0;
      for (Instance instance : p.getReadyToServeInstances()){
        if(instance.getNodeId().equals(nodeId)){
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

    List<String> topics = store.getVersions().stream().map((version) -> version.kafkaTopicName()).collect(Collectors.toList());
    for (String topic : topics) {
      routingRepository.subscribeRoutingDataChange(topic, this);
      //make sure we're up-to-date after registering as a listener
      this.onRoutingDataChanged(routingRepository.getPartitionAssignments(topic));
      oldTopics.remove(topic);
    }

    removeTopics(oldTopics);
  }

  private Set<String> getStoreTopics(String storeName){
    return storeVersionBuckets.keySet().stream()
        .filter((topic) -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .collect(Collectors.toSet());
  }

  private void removeTopics(Set<String> topicsToRemove){
    for (String topic : topicsToRemove){
      routingRepository.unSubscribeRoutingDataChange(topic, this);
      storeVersionBuckets.remove(topic);
    }
  }

  /**
   * For tests
   * @return
   */
  protected Set<String> listTopics(){
    return storeVersionBuckets.keySet();
  }
}
