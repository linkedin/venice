package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.RoutingDataRepository;
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

  public VeniceHostFinder(RoutingDataRepository dataRepository,
      boolean isStickyRoutingEnabledForSingleGet,
      boolean isStickyRoutingEnabledForMultiGet) {
    this.dataRepository = dataRepository;
    this.isStickyRoutingEnabledForSingleGet = isStickyRoutingEnabledForSingleGet;
    this.isStickyRoutingEnabledForMultiGet = isStickyRoutingEnabledForMultiGet;
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
       * Zero available host issue is handled by {@link VeniceDelegateMode} by checking whether there is any 'offline request'.
       */
      return hosts;
    }
    // hosts is an unmodifiable list
    List<Instance> newHosts = new ArrayList<>(hosts);
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
