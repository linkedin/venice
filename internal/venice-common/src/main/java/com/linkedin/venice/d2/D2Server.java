package com.linkedin.venice.d2;

import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class D2Server implements ServiceDiscoveryAnnouncer {
  private static final Logger LOG = LogManager.getLogger(D2Server.class);

  private final D2ServerManager _d2ServerManager;

  public D2Server(D2ServerManager d2ServerManager) {
    _d2ServerManager = d2ServerManager;
  }

  public void notifyShutdown() {
    try {
      _d2ServerManager.shutdown();
    } catch (Exception e) {
      throw new RuntimeException("Error shutting down D2ServerManager", e);
    }
  }

  public void notifyStartup() {
    try {
      _d2ServerManager.start();
    } catch (Exception e) {
      throw new RuntimeException("Error starting up D2ServerManager", e);
    }
  }

  /**
   * Manually start the d2 server.
   * This method will only take effects when automatic start up is disabled by _delayStart.
   */
  public void forceStart() {
    try {
      _d2ServerManager.forceStart();
    } catch (Exception e) {
      throw new RuntimeException("Error starting up D2ServerManager", e);
    }
  }

  public ZooKeeperAnnouncer[] getZkAnnouncers() {
    return _d2ServerManager.getZkAnnouncers();
  }

  public Set<String> getNodeUris() {
    return _d2ServerManager.getNodeUris();
  }

  public boolean isStarted() {
    return _d2ServerManager.isStarted();
  }

  public boolean isDelayStart() {
    return _d2ServerManager.isDelayStart();
  }

  /**
   * Compare D2Server to another one to check quality.
   * When ALL of the following conditions are met, two d2 servers are considered equal:
   * 1. having the same value in doNotStart.
   * 2. having the same zk connect string (connecting to the same zk cluster).
   * 3. having the same number of announcers. Note there should be exactly one announcer according to the current usage in {@link D2ServerFactory}.
   * 4. Announcers are the same with the same cluster name (announcing to the same cluster) AND same node uri (announcing for the same host).
   * @param other the other D2Server to check against.
   * @return true, if equal; false, if not.
   */
  public boolean isEqual(D2Server other) {
    if (other == null) {
      return false;
    }

    boolean isSameZk = this.getZkConnectString().equals(other.getZkConnectString());
    boolean isSameDoNotStart = this.isDoNotStart() == other.isDoNotStart();
    ZooKeeperAnnouncer[] thisAnnouncers = this.getAllAnnouncers();
    ZooKeeperAnnouncer[] otherAnnouncers = other.getAllAnnouncers();
    if (thisAnnouncers.length != otherAnnouncers.length) {
      return false;
    }

    List<String> sortedThisAnnouncerIDs = sortAnnouncerIDs(thisAnnouncers);
    List<String> sortedOtherAnnouncerIDs = sortAnnouncerIDs(otherAnnouncers);
    boolean isSameAnnouncers = sortedThisAnnouncerIDs.equals(sortedOtherAnnouncerIDs);
    if (!isSameDoNotStart && isSameZk && isSameAnnouncers) { // only doNotStart is different
      LOG.error(
          new IllegalArgumentException(
              "Found two D2 announcers with duplicate configs but only differ in doNotStart. "
                  + "Check your D2 announcement configs and remove the one with doNotStart=true. Duplicate entries:"
                  + getAnnouncingTargetID() + "\nAND " + other.getAnnouncingTargetID()));
    }
    return isSameZk && isSameDoNotStart && isSameAnnouncers;
  }

  /**
   * Get a ID string consists of the attributes that uniquely define the announcing target, which are:
   * 1) doNotStart 2) zkHosts 3) zk announcers (clusterName + nodeUri) -- Note that there should be exactly one announcer according
   * to the current usage in {@link D2ServerFactory}.
   * Sample ID string:
   * {
   * doNotStart=false,
   * zkHosts=zk.prod-lor1,
   * zkAnnouncers=[
   * {clusterName=cluster1, nodeUri=uri1}
   * ]}
   * @return a ID string of the announcing target.
   */
  public String getAnnouncingTargetID() {
    StringBuilder builder = new StringBuilder();
    builder.append("{\ndoNotStart=")
        .append(isDoNotStart())
        .append(",\n")
        .append("zkHosts=")
        .append(getZkConnectString())
        .append(",\n")
        .append("zkAnnouncers=[");

    ZooKeeperAnnouncer[] announcers = getAllAnnouncers();
    for (int x = 0; x < announcers.length; x++) {
      if (x > 0) {
        builder.append(",");
      }
      builder.append("\n").append(getAnnouncerID(announcers[x]));
      if (x == announcers.length - 1) {
        builder.append("\n");
      }
    }
    builder.append("]}");

    return builder.toString();
  }

  ///// Private Helpers /////

  // VisibleForTesting
  ZooKeeperAnnouncer[] getAllAnnouncers() {
    return _d2ServerManager.getZkAnnouncersWithoutStartCheck();
  }

  // VisibleForTesting
  String getZkConnectString() {
    return _d2ServerManager.getZkConnectString();
  }

  // VisibleForTesting
  boolean isDoNotStart() {
    return _d2ServerManager.isDoNotStart();
  }

  private String getAnnouncerID(ZooKeeperAnnouncer announcer) {
    return announcer == null ? "" : "{clusterName=" + announcer.getCluster() + ", nodeUri=" + announcer.getUri() + "}";
  }

  // get a list of sorted announcer ID strings
  private List<String> sortAnnouncerIDs(ZooKeeperAnnouncer[] announcers) {
    return Arrays.stream(announcers).map(this::getAnnouncerID).sorted().collect(Collectors.toList());
  }

  @Override
  public void register() {
    this.forceStart();
  }

  @Override
  public void unregister() {
    this.notifyShutdown();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder().append(this.getClass().getSimpleName()).append("{zkAnnouncers=[");
    ZooKeeperAnnouncer[] announcers = getAllAnnouncers();
    for (int x = 0; x < announcers.length; x++) {
      if (x > 0) {
        builder.append(", ");
      }
      builder.append(getAnnouncerID(announcers[x]));
    }
    builder.append("]}");
    return builder.toString();
  }
}
