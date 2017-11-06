package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.utils.ExpiringSet;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


public class VeniceHostHealth implements HostHealthMonitor<Instance> {

  private static final Logger logger = Logger.getLogger(VeniceHostHealth.class);
  private static final long UNHEALTHY_SECONDS = 20L;

  private ExpiringSet<String> slowPartitionHosts = new ExpiringSet<>(UNHEALTHY_SECONDS, TimeUnit.SECONDS);
  private ExpiringSet<String> unhealthyHosts = new ExpiringSet<>(UNHEALTHY_SECONDS, TimeUnit.SECONDS);

  private final LiveInstanceMonitor liveInstanceMonitor;

  public VeniceHostHealth(LiveInstanceMonitor liveInstanceMonitor) {
    this.liveInstanceMonitor = liveInstanceMonitor;
  }

  /**
   * Indicate that a partition on a host has fallen behind in consumption and should not be used
   * for queries for that partition.
   *
   * @param hostName
   * @param partitionName
   */
  public void setPartitionAsSlow(Instance hostName, String partitionName){
    String identifier = hostPartitionString(hostName, partitionName);
    slowPartitionHosts.add(identifier);
    logger.info(identifier + " is slow, marking as unhealthy for " + UNHEALTHY_SECONDS + " seconds");
  }

  /**
   * Mark that something is wrong with an entire host and it should not be used for queries.
   *
   * @param hostName
   */
  public void setHostAsUnhealthy(Instance hostName){
    String identifier = hostName.getUrl();
    unhealthyHosts.add(identifier);
    logger.info("Marking " + identifier + " as unhealthy for " + UNHEALTHY_SECONDS + " seconds");
  }

  @Override
  public boolean isHostHealthy(Instance hostName, String partitionName) {
    if (!liveInstanceMonitor.isInstanceAlive(hostName) // not alive
        || slowPartitionHosts.contains(hostPartitionString(hostName, partitionName))
        || unhealthyHosts.contains(hostName.getUrl())){
      return false; /* can't check-then-get, would cause a race condition and might get null */
    } else {
      return true;
    }
  }

  private static String hostPartitionString(Instance host, String partition){
    return host.getHost() + ":" + host.getPort() + "_" + partition;
  }
}
