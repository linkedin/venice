package com.linkedin.venice.router.api;

import static com.linkedin.venice.read.RequestType.*;

import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.utils.HelixUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceHostFinder implements HostFinder<Instance, VeniceRole> {
  private static final Logger logger = LogManager.getLogger(VeniceHostFinder.class);

  private final OnlineInstanceFinder onlineInstanceFinder;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final HostHealthMonitor<Instance> instanceHealthMonitor;

  public VeniceHostFinder(
      OnlineInstanceFinder onlineInstanceFinder,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      HostHealthMonitor<Instance> instanceHealthMonitor) {
    this.onlineInstanceFinder = onlineInstanceFinder;

    this.routerStats = routerStats;
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  /***
   * This parameter list is based on the router API.
   * The Venice router currently ignores all but the resourceName and partitionName
   *
   * @param requestMethod - used to identify the read request type
   * @param resourceName - required
   * @param partitionName - required
   * @param hostHealthMonitor - ignored, this class will use its own {@link HostHealthMonitor}
   * @param roles - ignored
   * @return
   */
  @Override
  public List<Instance> findHosts(
      String requestMethod,
      String resourceName,
      String partitionName,
      HostHealthMonitor<Instance> hostHealthMonitor,
      VeniceRole roles) {
    List<Instance> hosts =
        onlineInstanceFinder.getReadyToServeInstances(resourceName, HelixUtils.getPartitionId(partitionName));
    if (hosts.isEmpty()) {
      /**
       * Zero available host issue is handled by {@link VeniceDelegateMode} by checking whether there is any 'offline request'.
       */
      logger.warn("No ready-to-serve host for resource " + resourceName + " with partition " + partitionName);
      return hosts;
    }
    /**
     * Current {@link VeniceHostFinder} will always filter out unhealthy host when finding available hosts for current
     * request, and here are the reasons:
     * Health check will always happen here {@link VeniceHostFinder}
     */
    // hosts is an unmodifiable list
    List<Instance> newHosts = new ArrayList<>(hosts.size());
    boolean isSingleGet = VeniceRouterUtils.isHttpGet(requestMethod);
    /**
     * {@link VeniceHostFinder} needs to filter out unhealthy hosts.
     *
     * Metric to track unhealthy host request for both compute and multi-get will fall into the multi-get category
     * since right now, there is no good way to differentiate compute from multi-get according to the existing API
     * of {@link HostFinder}.
     */
    AggRouterHttpRequestStats currentStats = routerStats.getStatsByType(isSingleGet ? SINGLE_GET : MULTI_GET);
    /**
     * It seems not clean to use the following method to extract store name, but inside Venice, Kafka topic name is same
     * as Helix resource name.
     */
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    for (Instance instance: hosts) {
      // Filter out unhealthy hosts
      /**
       * Right now, partition-level health check by measuring offset lag is not enabled.
       * Router won't record unhealthy metric when {@link hostHealthMonitor} is returning unhealthy since
       * it is only being used for retry purpose, which means when {@link hostHealthMonitor} is returning false,
       * the current request is a retry request.
       */
      if (!instanceHealthMonitor.isHostHealthy(instance, partitionName)) {
        currentStats.recordFindUnhealthyHostRequest(storeName);
        continue;
      }
      if (hostHealthMonitor.isHostHealthy(instance, partitionName)) {
        newHosts.add(instance);
      }
    }
    final int hostCount = newHosts.size();
    if (hostCount <= 1) {
      if (hostCount == 0) {
        logger.warn(
            "All host(s) for resource " + resourceName + " with partition " + partitionName + " are not healthy: "
                + hosts);
      }
      return newHosts;
    }

    // Zero available host issue is handled by {@link VeniceDelegateMode} by checking whether there is any 'offline
    // request'.

    Collections.shuffle(newHosts); // Randomize order so that multiget using ScatterGatherMode.GROUP_BY_PRIMARY_HOST or
                                   // LEAST_LOADED_ROUTING results in an even distribution of partitions to hosts.
    return newHosts;
  }

  @Override
  public Collection<Instance> findAllHosts(VeniceRole roles) throws RouterException {
    throw new RouterException(
        HttpResponseStatus.class,
        HttpResponseStatus.BAD_REQUEST,
        HttpResponseStatus.BAD_REQUEST.code(),
        "Find All Hosts is not a supported operation",
        true);
  }
}
