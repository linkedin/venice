package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.notifier.PushMonitorNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;
import java.util.concurrent.CompletableFuture;
import org.apache.zookeeper.KeeperException;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService implements StatusMessageHandler<KillOfflinePushMessage> {

  private static final Logger logger = Logger.getLogger(HelixParticipationService.class);

  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private static final int MAX_RETRY = 30;
  private static final int RETRY_INTERVAL_SEC = 1;

  private final Instance instance;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StateModelFactory stateModelFactory;
  private final StoreIngestionService ingestionService;
  private final StorageService storageService;
  private final VeniceConfigLoader veniceConfigLoader;

  private HelixManager manager;

  private ThreadPoolExecutor helixStateTransitionExecutorService;

  private HelixStatusMessageChannel messageChannel;

  public HelixParticipationService(@NotNull StoreIngestionService storeIngestionService,
          @NotNull StorageService storageService,
          @NotNull VeniceConfigLoader veniceConfigLoader,
          @NotNull String zkAddress,
          @NotNull String clusterName,
          int port) {
    this.ingestionService = storeIngestionService;
    this.storageService = storageService;
    this.clusterName = clusterName;
    //The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    this.participantName = Utils.getHelixNodeIdentifier(port);
    this.zkAddress = zkAddress;
    this.veniceConfigLoader = veniceConfigLoader;
    instance = new Instance(participantName, Utils.getHostName(), port);
    // Create a thread pool used to execute incoming state transitions. Set corePoolSize and maxPoolSize as the same
    // value, but enable allowCoreThreadTimeOut. So the expected behavior is pool will create a new thread if the number
    // of running threads is fewer than corePoolSize, otherwise add this task into queue. If a thread is idle for more
    // than 300 seconds, pool will collect this thread.
    helixStateTransitionExecutorService =
        new ThreadPoolExecutor(veniceConfigLoader.getVeniceServerConfig().getMaxStateTransitionThreadNumber(),
            veniceConfigLoader.getVeniceServerConfig().getMaxStateTransitionThreadNumber(), 300L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), new DaemonThreadFactory("venice-state-transition"));
    helixStateTransitionExecutorService.allowCoreThreadTimeOut(true);

    stateModelFactory = new VeniceStateModelFactory(storeIngestionService, storageService, veniceConfigLoader,
        helixStateTransitionExecutorService);
  }

  @Override
  public boolean startInner() {
    logger.info("Attempting to start HelixParticipation service");
    manager = HelixManagerFactory
            .getZKHelixManager(clusterName, this.participantName, InstanceType.PARTICIPANT,
                zkAddress);
    manager.getStateMachineEngine().registerStateModelFactory(STATE_MODEL_REFERENCE_NAME, stateModelFactory);
    //TODO Now Helix instance config only support host and port. After talking to Helix team, they will add
    // customize k-v data support soon. Then we don't need LiveInstanceInfoProvider here. Use the instance config
    // is a better way because it reduce the communication times to Helix. Other wise client need to get  thsi
    // information from ZK in the extra request and response.
    LiveInstanceInfoProvider liveInstanceInfoProvider = () -> {
      // serialize serviceMetadata to ZNRecord
      return HelixInstanceConverter.convertInstanceToZNRecord(instance);
    };
    manager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);

    // Create a message channel to receive messagte from controller.
    messageChannel = new HelixStatusMessageChannel(manager);
    messageChannel.registerHandler(KillOfflinePushMessage.class, this);

    //TODO Venice Listener should not be started, until the HelixService is started.
    asyncStart();

    // The start up process may not be finished yet, because it is continuing asynchronously.
    return false;
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }

  private void checkBeforeJoinInCluster() {
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    try {
      // Check whether the cluster is ready or not at first to prevent zk no node exception.
      HelixUtils.checkClusterSetup(admin, clusterName, MAX_RETRY, RETRY_INTERVAL_SEC);
      List<String> instances = admin.getInstancesInCluster(clusterName);
      if (instances.contains(instance.getNodeId())) {
        logger.info(instance.getNodeId() + " is not a new node to cluster: " + clusterName + ", skip the cleaning up.");
        return;
      }
      // Could not get instance from helix cluster. So it's a new machine or the machine which had been removed from
      // this cluster. In order to prevent resource leaking, we need to clean up all legacy stores.
      logger.info(instance.getNodeId() + " is a new node or had been removed from cluster: " + clusterName + " start cleaning up local storage.");
      storageService.cleanupAllStores(veniceConfigLoader);
      logger.info("Cleaning up complete, " + instance.getNodeId() + " can now join cluster:" + clusterName);
    }finally {
      admin.close();
    }
  }


  /**
   * check RouterServer#asyncStart() for details about asyncStart
   */
  private void asyncStart() {
    CompletableFuture.runAsync(() -> {
      try {
        // Check node status before joining the cluster.
        // TODO Helix team will provide a way to let us do some checking after connecting to Helix.
        // TODO In that case, it's guaranteed that the node would not be assigned with any resource before we completed our
        // TODO checking, so we could use HelixManager to get some metadata instead of creating a new zk connection.
        checkBeforeJoinInCluster();
        manager.connect();
        //setup instance config for participant used by CRUSH alg
        HelixUtils.setupInstanceConfig(clusterName, instance.getNodeId(), zkAddress);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        logger.error("Venice server is about to close");

        //Since helix manager is necessary. We force to exit the program if it is not able to connected.
        System.exit(1);
      }

      // Record replica status in Zookeeper.
      PushMonitorNotifier pushMonitorNotifier = new PushMonitorNotifier(
          new HelixOfflinePushMonitorAccessor(clusterName, new ZkClient(zkAddress), new HelixAdapterSerializer()),
          instance.getNodeId());
      ingestionService.addNotifier(pushMonitorNotifier);

      serviceState.set(ServiceState.STARTED);

      logger.info("Successfully started Helix Participation Service");
    });
  }

  @Override
  public void handleMessage(KillOfflinePushMessage message) {
    VeniceStoreConfig storeConfig = veniceConfigLoader.getStoreConfig(message.getKafkaTopic());
    if (ingestionService.containsRunningConsumption(storeConfig)) {
      //push is failed, stop consumption.
      logger.info("Receive the message to kill consumption for topic:" + message.getKafkaTopic());
      ingestionService.killConsumptionTask(storeConfig);
      logger.info("Killed Consumption for topic:" + message.getKafkaTopic());
    }else{
      logger.info("Ignore the kill message for topic:" + message.getKafkaTopic());
    }
  }
}
