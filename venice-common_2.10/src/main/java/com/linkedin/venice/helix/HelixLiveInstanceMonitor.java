package com.linkedin.venice.helix;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * This class is used to monitor the current live instances(Helix participators).
 *
 * Here, it is not using {@link org.apache.helix.HelixManager} because HelixManager is using the same
 * {@link ZkClient} being used for all the Helix related ZK Watches/data operations.
 * When there are a lot of state transitions during node starts/stops, it will take a very long time to let
 * HelixManager to report the instance list change since those state transition notifications could happen
 * earlier than node stop/start event. Under this circumstance, HelixManager won't report node list change
 * in time. Using a separate {@link ZkClient} will allow node list change notification happen in a separate
 * channel, so that node list change can be captured in time.
 *
 * This way is a little hacky since this class is reading from Helix ZK directory directly.
 */
public class HelixLiveInstanceMonitor implements IZkChildListener, VeniceResource, LiveInstanceMonitor {
  private static final Logger LOGGER = Logger.getLogger(HelixLiveInstanceMonitor.class);
  private static final String LIVE_INSTANCE_PATH = "/LIVEINSTANCES";
  private final String clusterName;
  private final ZkClient zkClient;
  private Set<Instance> liveInstanceSet = Collections.emptySet();
  private final CachedResourceZkStateListener zkStateListener;


  public HelixLiveInstanceMonitor(ZkClient zkClient, String clusterName) {
    this.zkClient = zkClient;
    this.clusterName = clusterName;
    this.zkStateListener = new CachedResourceZkStateListener(this);
  }

  private String getLiveInstanceRootPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + LIVE_INSTANCE_PATH;
  }

  @Override
  public void refresh() {
    // subscribe is thread safe method.
    zkClient.subscribeStateChanges(zkStateListener);
    zkClient.subscribeChildChanges(getLiveInstanceRootPath(), this);
    /**
     * Here is using {@link ZkClient#getChildren(String)}, which should either success or fail.
     * It should not be possible to receive partial data here.
     */
    List<String> instances = zkClient.getChildren(getLiveInstanceRootPath());
    liveInstanceSet = convertToInstance(instances);
    LOGGER.info("Got live instances: " + liveInstanceSet);
  }

  private Set<Instance> convertToInstance(List<String> instances) {
    Set<Instance> newInstanceSet = new HashSet<>();
    instances.forEach(instance -> newInstanceSet.add(HelixUtils.getInstanceFromHelixInstanceName(instance)));
    return Collections.unmodifiableSet(newInstanceSet);
  }

  @Override
  public void clear() {
    // un-subscribe is thread safe method
    zkClient.unsubscribeStateChanges(zkStateListener);
    zkClient.unsubscribeChildChanges(getLiveInstanceRootPath(), this);
    liveInstanceSet = Collections.emptySet();
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
    liveInstanceSet = convertToInstance(currentChildren);
    LOGGER.info("Received new live instance set: " + liveInstanceSet);
  }

  @Override
  public boolean isInstanceAlive(Instance instance) {
    return liveInstanceSet.contains(instance);
  }

  @Override
  public Set<Instance> getAllLiveInstances() {
    return Collections.unmodifiableSet(liveInstanceSet);
  }
}
