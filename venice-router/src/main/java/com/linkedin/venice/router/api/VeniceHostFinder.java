package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.utils.HelixUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class VeniceHostFinder implements HostFinder<Instance, VeniceRole> {
  private final RoutingDataRepository dataRepository;
  private final boolean isStickyRoutingEnabledForSingleGet;
  private final boolean isStickyRoutingEnabledForMultiGet;
  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;

  public VeniceHostFinder(RoutingDataRepository dataRepository,
      boolean isStickyRoutingEnabledForSingleGet,
      boolean isStickyRoutingEnabledForMultiGet,
      AggRouterHttpRequestStats statsForSingleGet,
      AggRouterHttpRequestStats statsForMultiGet) {
    this.dataRepository = dataRepository;
    this.isStickyRoutingEnabledForSingleGet = isStickyRoutingEnabledForSingleGet;
    this.isStickyRoutingEnabledForMultiGet = isStickyRoutingEnabledForMultiGet;
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
  }

  /***
   * This parameter list is based on the router API.
   * The Venice router currently ignores all but the resourceName and partitionName
   *
   * @param requestMethod - ignored
   * @param resourceName - required
   * @param partitionName - required
   * @param hostHealthMonitor - ignored
   * @param roles - ignored
   * @return
   */
  @Override
  public List<Instance> findHosts(String requestMethod, String resourceName, String partitionName,
      HostHealthMonitor<Instance> hostHealthMonitor, VeniceRole roles) {
    List<Instance> hosts = dataRepository.getReadyToServeInstances(resourceName, HelixUtils.getPartitionId(partitionName));
    int hostCount = hosts.size();
    if (hostCount <= 1 ||
        // single-get but sticky routing is not enabled
        VeniceRouterUtils.isHttpGet(requestMethod) && !isStickyRoutingEnabledForSingleGet ||
        // multi-get but sticky routing is not enabled
        VeniceRouterUtils.isHttpPost(requestMethod) && !isStickyRoutingEnabledForMultiGet) {
      /**
       * No need to filter out unhealthy hosts in this case since {@link com.linkedin.ddsstorage.router.api.ScatterGatherMode}
       * will take care of it.
       */
      /**
       * Zero available host issue is handled by {@link VeniceDelegateMode} by checking whether there is any 'offline request'.
       */
      return hosts;
    }
    // hosts is an unmodifiable list
    List<Instance> newHosts = new ArrayList<>();
    /**
     * {@link VeniceHostFinder} needs to filter out unhealthy hosts if there are more than 1 available hosts.
     * Otherwise, the unhealthy host could keep receiving request because of sticky routing.
     */
    AggRouterHttpRequestStats currentStats = VeniceRouterUtils.isHttpGet(requestMethod) ? statsForSingleGet : statsForMultiGet;
    /**
     * It seems not clean to use the following method to extract store name, but inside Venice, Kafka topic name is same
     * as Helix resource name.
     */
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    for (Instance instance : hosts) {
      // Filter out unhealthy hosts
      /**
       * Right now, partition-level healthy check by measuring offset lag is not enabled.
       * When we want to enable it, we need to think about it very carefully since it could impact sticky routing performance
       * since the available hosts for a specified partition of a hybrid store could change continuously.
       *
       * TODO: Sticky routing might be more preferable for hybrid store than measuring offset lag.
       */
      if (hostHealthMonitor.isHostHealthy(instance, partitionName)) {
        newHosts.add(instance);
      } else {
        currentStats.recordFindUnhealthyHostRequest(storeName);
      }
    }
    hostCount = newHosts.size();
    if (hostCount <= 1) {
      /**
       * Zero available host issue is handled by {@link VeniceDelegateMode} by checking whether there is any 'offline request'.
       */
      return newHosts;
    }
    newHosts.sort(Comparator.comparing(Instance::getNodeId));
    int partitionId = HelixUtils.getPartitionId(partitionName);
    int chosenIndex = partitionId % hostCount;
    return Arrays.asList(newHosts.get(chosenIndex));
  }

  @Override
  public Collection<Instance> findAllHosts(VeniceRole roles) throws RouterException {
    throw new RouterException(HttpResponseStatus.class, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.code(),
        "Find All Hosts is not a supported operation", true);
  }
}
