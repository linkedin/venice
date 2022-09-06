package com.linkedin.venice.helix;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.api.listeners.ConfigChangeListener;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.CurrentStateChangeListener;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.MessageListener;
import org.apache.helix.api.listeners.ResourceConfigChangeListener;
import org.apache.helix.api.listeners.ScopedConfigChangeListener;
import org.apache.helix.healthcheck.ParticipantHealthReportCollector;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * A wrapper around {@link HelixManager} which disables the unsafe functionality.
 *
 * Venice should never use a {@link HelixManager} directly. All uses of this class should
 * always be wrapped in a {@link SafeHelixManager}.
 *
 * In order to ensure a compile-time guarantee that only safe APIs are called, we explicitly
 * choose to remove the unsafe APIs from the interface. Therefore, we do not actually
 * implement the {@link HelixManager} interface. Only one unsafe API is removed:
 *
 * - {@link HelixManager#getHelixDataAccessor}
 *     It is replaced by a similar function that returns a {@link SafeHelixDataAccessor} instead.
 *
 * Besides the unsafe APIs, this class also takes away all of the APIs which Helix itself
 * declares to be deprecated. These are the removed deprecated APIs:
 *
 * - {@link HelixManager#addIdealStateChangeListener(org.apache.helix.IdealStateChangeListener)}
 * - {@link HelixManager#addLiveInstanceChangeListener(org.apache.helix.LiveInstanceChangeListener)}
 * - {@link HelixManager#addConfigChangeListener(ConfigChangeListener)}
 * - {@link HelixManager#addInstanceConfigChangeListener(org.apache.helix.InstanceConfigChangeListener)}
 * - {@link HelixManager#addConfigChangeListener(org.apache.helix.ScopedConfigChangeListener, HelixConfigScope.ConfigScopeProperty)}
 * - {@link HelixManager#addMessageListener(org.apache.helix.MessageListener, String)}
 * - {@link HelixManager#addCurrentStateChangeListener(org.apache.helix.CurrentStateChangeListener, String, String)}
 * - {@link HelixManager#addExternalViewChangeListener(org.apache.helix.ExternalViewChangeListener)}
 * - {@link HelixManager#addControllerListener(org.apache.helix.ControllerChangeListener)}
 * - {@link HelixManager#addControllerMessageListener(org.apache.helix.MessageListener)}
 *
 * TODO: Review the rest of the functions and determine if more should be culled.
 */
public class SafeHelixManager {
  private final HelixManager helixManager;

  public SafeHelixManager(HelixManager helixManager) {
    this.helixManager = helixManager;
  }

  public void connect() throws Exception {
    helixManager.connect();
  }

  public boolean isConnected() {
    return helixManager.isConnected();
  }

  public void disconnect() {
    helixManager.disconnect();
  }

  public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception {
    helixManager.addIdealStateChangeListener(listener);
  }

  public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception {
    helixManager.addLiveInstanceChangeListener(listener);
  }

  public void addInstanceConfigChangeListener(InstanceConfigChangeListener listener) throws Exception {
    helixManager.addInstanceConfigChangeListener(listener);
  }

  public void addResourceConfigChangeListener(ResourceConfigChangeListener listener) throws Exception {
    helixManager.addResourceConfigChangeListener(listener);
  }

  public void addClusterfigChangeListener(ClusterConfigChangeListener listener) throws Exception {
    helixManager.addClusterfigChangeListener(listener);
  }

  public void addConfigChangeListener(ScopedConfigChangeListener listener, HelixConfigScope.ConfigScopeProperty scope)
      throws Exception {
    helixManager.addConfigChangeListener(listener, scope);
  }

  public void addMessageListener(MessageListener listener, String instanceName) throws Exception {
    helixManager.addMessageListener(listener, instanceName);
  }

  public void addCurrentStateChangeListener(CurrentStateChangeListener listener, String instanceName, String sessionId)
      throws Exception {
    helixManager.addCurrentStateChangeListener(listener, instanceName, sessionId);
  }

  public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    helixManager.addExternalViewChangeListener(listener);
  }

  public void addTargetExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception {
    helixManager.addTargetExternalViewChangeListener(listener);
  }

  public void addControllerListener(ControllerChangeListener listener) {
    helixManager.addControllerListener(listener);
  }

  public void addControllerMessageListener(MessageListener listener) {
    helixManager.addControllerMessageListener(listener);
  }

  public boolean removeListener(PropertyKey key, Object listener) {
    return helixManager.removeListener(key, listener);
  }

  /**
   * This function is incompatible with the {@link HelixManager} interface, since the return type
   * of {@link SafeHelixDataAccessor} is not compatible with the return of the unsafe interface,
   * which is a {@link HelixDataAccessor}.
   */
  public SafeHelixDataAccessor getHelixDataAccessor() {
    return new SafeHelixDataAccessor(helixManager.getHelixDataAccessor());
  }

  public ConfigAccessor getConfigAccessor() {
    return helixManager.getConfigAccessor();
  }

  public String getClusterName() {
    return helixManager.getClusterName();
  }

  public String getInstanceName() {
    return helixManager.getInstanceName();
  }

  public String getSessionId() {
    return helixManager.getSessionId();
  }

  public long getLastNotificationTime() {
    return helixManager.getLastNotificationTime();
  }

  public HelixAdmin getClusterManagmentTool() {
    return helixManager.getClusterManagmentTool();
  }

  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return helixManager.getHelixPropertyStore();
  }

  public ClusterMessagingService getMessagingService() {
    return helixManager.getMessagingService();
  }

  public InstanceType getInstanceType() {
    return helixManager.getInstanceType();
  }

  public String getVersion() {
    return helixManager.getVersion();
  }

  public HelixManagerProperties getProperties() {
    return helixManager.getProperties();
  }

  public StateMachineEngine getStateMachineEngine() {
    return helixManager.getStateMachineEngine();
  }

  public Long getSessionStartTime() {
    return helixManager.getSessionStartTime();
  }

  public boolean isLeader() {
    return helixManager.isLeader();
  }

  public void startTimerTasks() {
    helixManager.startTimerTasks();
  }

  public void stopTimerTasks() {
    helixManager.stopTimerTasks();
  }

  public void addPreConnectCallback(PreConnectCallback callback) {
    helixManager.addPreConnectCallback(callback);
  }

  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    helixManager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
  }

  public ParticipantHealthReportCollector getHealthReportCollector() {
    return helixManager.getHealthReportCollector();
  }

  public HelixManager getOriginalManager() {
    return helixManager;
  }
}
