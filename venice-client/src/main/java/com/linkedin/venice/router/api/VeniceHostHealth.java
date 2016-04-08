package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.ExpiringSet;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * Created by mwise on 4/8/16.
 */
public class VeniceHostHealth implements HostHealthMonitor<Instance> {

  private static final Logger logger = Logger.getLogger(VeniceHostHealth.class);
  private static final long UNHEALTHY_SECONDS = 20L;

  private ExpiringSet<String> slowPartitionHosts = new ExpiringSet(UNHEALTHY_SECONDS, TimeUnit.SECONDS);

  public void setHostAsSlow(Instance hostName, String partitionName){
    String identifier = hostPartitionString(hostName, partitionName);
    slowPartitionHosts.add(identifier);
    logger.info(identifier + " is slow, marking as unhealthy for " + UNHEALTHY_SECONDS + " seconds");
  }

  @Override
  public boolean isHostHealthy(Instance hostName, String partitionName) {
    if (slowPartitionHosts.contains(hostPartitionString(hostName, partitionName))){
      return false; /* can't check-then-get, would cause a race condition and might get null */
    } else {
      return true;
    }
  }

  private static String hostPartitionString(Instance host, String partition){
    return host.getHost() + ":" + host.getHttpPort() + "_" + partition;
  }
}
