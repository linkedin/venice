package com.linkedin.venice.router;

import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.Collection;
import java.util.List;


public class VeniceHostFinder implements HostFinder<Instance, VeniceRole> {

  private RoutingDataRepository dataRepository;

  public VeniceHostFinder(RoutingDataRepository dataRepository){
    this.dataRepository = dataRepository;
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
    return dataRepository.getInstances(resourceName, Partition.getPartitionIdFromName(partitionName));
  }

  @Override
  public Collection<Instance> findAllHosts(VeniceRole roles) throws VeniceRouterException {
    throw new VeniceRouterException("Find All Hosts is not a supported operation");
  }
}
