package com.linkedin.davinci.helix;

import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_DERIVED_SCHEMA_ID;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.PartitionPushStatusNotifier;
import com.linkedin.davinci.notifier.PushMonitorNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.ThreadPoolStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixInstanceConverter;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService
    implements StatusMessageHandler<KillOfflinePushMessage> {
  private static final Logger LOGGER = LogManager.getLogger(HelixParticipationService.class);

  private static final int MAX_RETRY = 30;
  private static final int RETRY_INTERVAL_SEC = 1;

  private final Instance instance;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StoreIngestionService ingestionService;
  private final StorageService storageService;
  private final VeniceConfigLoader veniceConfigLoader;
  private final ReadOnlyStoreRepository helixReadOnlyStoreRepository;
  private final MetricsRepository metricsRepository;
  private final VeniceIngestionBackend ingestionBackend;
  private final CompletableFuture<SafeHelixManager> managerFuture; // complete this future when the manager is connected
  private final CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;
  private PushStatusStoreWriter statusStoreWriter;
  private ZkClient zkClient;
  private SafeHelixManager helixManager;
  private AbstractStateModelFactory leaderFollowerParticipantModelFactory;
  private HelixPartitionStatusAccessor partitionPushStatusAccessor;
  private ThreadPoolExecutor leaderFollowerHelixStateTransitionThreadPool;
  private VeniceOfflinePushMonitorAccessor veniceOfflinePushMonitorAccessor;

  // This is ONLY for testing purpose.
  public ThreadPoolExecutor getLeaderFollowerHelixStateTransitionThreadPool() {
    return leaderFollowerHelixStateTransitionThreadPool;
  }

  public HelixParticipationService(
      StoreIngestionService storeIngestionService,
      StorageService storageService,
      StorageMetadataService storageMetadataService,
      VeniceConfigLoader veniceConfigLoader,
      ReadOnlyStoreRepository helixReadOnlyStoreRepository,
      MetricsRepository metricsRepository,
      String zkAddress,
      String clusterName,
      int port,
      String hostname,
      CompletableFuture<SafeHelixManager> managerFuture) {
    this.ingestionService = storeIngestionService;
    this.storageService = storageService;
    this.clusterName = clusterName;
    // The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    this.participantName = Utils.getHelixNodeIdentifier(hostname, port);
    this.zkAddress = zkAddress;
    this.veniceConfigLoader = veniceConfigLoader;
    this.helixReadOnlyStoreRepository = helixReadOnlyStoreRepository;
    this.metricsRepository = metricsRepository;
    this.instance = new Instance(participantName, hostname, port);
    this.managerFuture = managerFuture;
    this.partitionPushStatusAccessorFuture = new CompletableFuture<>();
    if (!(storeIngestionService instanceof KafkaStoreIngestionService)) {
      throw new VeniceException("Expecting " + KafkaStoreIngestionService.class.getName() + " for ingestion backend!");
    }
    if (veniceConfigLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED)) {
      this.ingestionBackend = new IsolatedIngestionBackend(
          veniceConfigLoader,
          helixReadOnlyStoreRepository,
          metricsRepository,
          storageMetadataService,
          (KafkaStoreIngestionService) storeIngestionService,
          storageService);
    } else {
      this.ingestionBackend = new DefaultIngestionBackend(
          storageMetadataService,
          (KafkaStoreIngestionService) storeIngestionService,
          storageService);
    }
  }

  // Set corePoolSize and maxPoolSize as the same value, but enable allowCoreThreadTimeOut. So the expected
  // behavior is pool will create a new thread if the number of running threads is fewer than corePoolSize, otherwise
  // add this task into queue. If a thread is idle for more than 300 seconds, pool will collect this thread.
  private ThreadPoolExecutor initHelixStateTransitionThreadPool(
      int size,
      String threadName,
      MetricsRepository metricsRepository,
      String statsName) {
    ThreadPoolExecutor helixStateTransitionThreadPool = new ThreadPoolExecutor(
        size,
        size,
        300L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory(threadName));
    helixStateTransitionThreadPool.allowCoreThreadTimeOut(true);

    // register stats that tracks the thread pool
    new ThreadPoolStats(metricsRepository, helixStateTransitionThreadPool, statsName);

    return helixStateTransitionThreadPool;
  }

  @Override
  public boolean startInner() {
    LOGGER.info("Attempting to start HelixParticipation service");
    helixManager = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, this.participantName, InstanceType.PARTICIPANT, zkAddress));

    VeniceServerConfig config = veniceConfigLoader.getVeniceServerConfig();
    leaderFollowerHelixStateTransitionThreadPool = initHelixStateTransitionThreadPool(
        config.getMaxLeaderFollowerStateTransitionThreadNumber(),
        "Venice-L/F-state-transition",
        metricsRepository,
        "Venice_L/F_ST_thread_pool");

    if (config.getLeaderFollowerThreadPoolStrategy()
        .equals(LeaderFollowerPartitionStateModelFactory.LeaderFollowerThreadPoolStrategy.DUAL_POOL_STRATEGY)) {
      leaderFollowerParticipantModelFactory = new LeaderFollowerPartitionStateModelDualPoolFactory(
          ingestionBackend,
          veniceConfigLoader,
          leaderFollowerHelixStateTransitionThreadPool,
          initHelixStateTransitionThreadPool(
              config.getMaxFutureVersionLeaderFollowerStateTransitionThreadNumber(),
              "venice-L/F-state-transition-future-version",
              metricsRepository,
              "Venice_L/F_ST_thread_pool_future_version"),
          helixReadOnlyStoreRepository,
          partitionPushStatusAccessorFuture,
          instance.getNodeId());
    } else {
      leaderFollowerParticipantModelFactory = new LeaderFollowerPartitionStateModelFactory(
          ingestionBackend,
          veniceConfigLoader,
          leaderFollowerHelixStateTransitionThreadPool,
          helixReadOnlyStoreRepository,
          partitionPushStatusAccessorFuture,
          instance.getNodeId());
    }
    LOGGER.info(
        "LeaderFollower threadPool info: strategy = {}, max future state transition thread = {}",
        config.getLeaderFollowerThreadPoolStrategy(),
        config.getMaxFutureVersionLeaderFollowerStateTransitionThreadNumber());

    helixManager.getStateMachineEngine()
        .registerStateModelFactory(LeaderStandbySMD.name, leaderFollowerParticipantModelFactory);
    // TODO Now Helix instance config only support host and port. After talking to Helix team, they will add
    // customize k-v data support soon. Then we don't need LiveInstanceInfoProvider here. Use the instance config
    // is a better way because it reduce the communication times to Helix. Other wise client need to get thsi
    // information from ZK in the extra request and response.
    LiveInstanceInfoProvider liveInstanceInfoProvider = () -> {
      // serialize serviceMetadata to ZNRecord
      return HelixInstanceConverter.convertInstanceToZNRecord(instance);
    };
    helixManager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);

    // Create a message channel to receive message from controller.
    HelixStatusMessageChannel messageChannel =
        new HelixStatusMessageChannel(helixManager, new HelixMessageChannelStats(metricsRepository, clusterName));
    messageChannel.registerHandler(KillOfflinePushMessage.class, this);

    // TODO Venice Listener should not be started, until the HelixService is started.
    asyncStart();

    // The start up process may not be finished yet, because it is continuing asynchronously.
    return false;
  }

  @Override
  public void stopInner() throws IOException {
    LOGGER.info("Attempting to stop HelixParticipation service.");
    if (helixManager != null) {
      try {
        helixManager.disconnect();
        LOGGER.info("Disconnected Helix Manager.");
      } catch (Exception e) {
        LOGGER.error(
            "Swallowed an exception while trying to disconnect the {}",
            helixManager.getClass().getSimpleName(),
            e);
      }
    } else {
      LOGGER.info("Helix Manager is null.");
    }
    ingestionBackend.close();
    LOGGER.info("Closed VeniceIngestionBackend.");
    leaderFollowerParticipantModelFactory.shutDownExecutor();

    try {
      leaderFollowerParticipantModelFactory.waitExecutorTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (zkClient != null) {
      LOGGER.info("Start closing ZkClient.");
      zkClient.close();
      LOGGER.info("Closed ZkClient.");
    }
    LOGGER.info("Finished stopping HelixParticipation service.");
  }

  private void checkBeforeJoinInCluster() {
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    try {
      // Check whether the cluster is ready or not at first to prevent zk no node exception.
      HelixUtils.checkClusterSetup(admin, clusterName, MAX_RETRY, RETRY_INTERVAL_SEC);
      List<String> instances = admin.getInstancesInCluster(clusterName);
      if (instances.contains(instance.getNodeId())) {
        LOGGER.info("{} is not a new node to cluster: {}, skip the cleaning up.", instance.getNodeId(), clusterName);
        return;
      }
      // Could not get instance from helix cluster. So it's a new machine or the machine which had been removed from
      // this cluster. In order to prevent resource leaking, we need to clean up all legacy stores.
      LOGGER.info(
          "{} is a new node or had been removed from cluster: {} start cleaning up local storage.",
          instance.getNodeId(),
          clusterName);
      storageService.cleanupAllStores(veniceConfigLoader);
      LOGGER.info("Cleaning up complete, {} can now join cluster: {}", instance.getNodeId(), clusterName);
    } finally {
      admin.close();
    }
  }

  /**
   * check RouterServer#asyncStart() for details about asyncStart
   * The difference between router async start and here is router asyncStart is disabled in prod since it is risky,
   * and it is only enabled in dev env for PCL.
   * For server, the deployment hook will make sure each partition of current version must have
   * enough ready-to-serve replicas before restarting next storage node.
   */
  private void asyncStart() {
    zkClient = ZkClientFactory.newZkClient(zkAddress);

    // we use push status store for persisting incremental push statuses
    VeniceProperties veniceProperties = veniceConfigLoader.getVeniceServerConfig().getClusterProperties();
    VeniceWriterFactory writerFactory = new VeniceWriterFactory(veniceProperties.toProperties());
    statusStoreWriter = new PushStatusStoreWriter(
        writerFactory,
        instance.getNodeId(),
        veniceProperties.getInt(PUSH_STATUS_STORE_DERIVED_SCHEMA_ID, 1));

    // Record replica status in Zookeeper.
    // Need to be started before connecting to ZK, otherwise some notification will not be sent by this notifier.
    veniceOfflinePushMonitorAccessor = new VeniceOfflinePushMonitorAccessor(
        clusterName,
        zkClient,
        new HelixAdapterSerializer(),
        veniceConfigLoader.getVeniceClusterConfig().getRefreshAttemptsForZkReconnect(),
        veniceConfigLoader.getVeniceClusterConfig().getRefreshIntervalForZkReconnectInMs());

    PushMonitorNotifier pushMonitorNotifier = new PushMonitorNotifier(
        veniceOfflinePushMonitorAccessor,
        statusStoreWriter,
        helixReadOnlyStoreRepository,
        instance.getNodeId());

    ingestionBackend.addPushStatusNotifier(pushMonitorNotifier);

    CompletableFuture.runAsync(() -> {
      try {
        // Check node status before joining the cluster.
        // TODO Helix team will provide a way to let us do some checking after connecting to Helix.
        // TODO In that case, it's guaranteed that the node would not be assigned with any resource before we completed
        // our
        // TODO checking, so we could use HelixManager to get some metadata instead of creating a new zk connection.
        checkBeforeJoinInCluster();
        helixManager.connect();
        managerFuture.complete(helixManager);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        LOGGER.error("Venice server is about to close");
        Utils.exit("Failed to start HelixParticipationService");
      }

      /**
       * The accessor can only get created successfully after helix manager is created.
       */
      partitionPushStatusAccessor = new HelixPartitionStatusAccessor(
          helixManager.getOriginalManager(),
          instance.getNodeId(),
          veniceConfigLoader.getVeniceServerConfig().isHelixHybridStoreQuotaEnabled());
      PartitionPushStatusNotifier partitionPushStatusNotifier =
          new PartitionPushStatusNotifier(partitionPushStatusAccessor);
      ingestionBackend.addPushStatusNotifier(partitionPushStatusNotifier);
      /**
       * Complete the accessor future after the accessor is created && the notifier is added.
       * This is for blocking the {@link AbstractPartitionStateModel #setupNewStorePartition()} until
       * the notifier is added.
       */
      partitionPushStatusAccessorFuture.complete(partitionPushStatusAccessor);
      LOGGER.info("Successfully started Helix partition status accessor.");

      serviceState.set(ServiceState.STARTED);
      LOGGER.info("Successfully started Helix Participation Service.");
    });
  }

  // test only
  public void replaceAndAddTestIngestionNotifier(VeniceNotifier notifier) {
    ingestionBackend.replaceAndAddTestPushStatusNotifier(notifier);
  }

  public Instance getInstance() {
    return instance;
  }

  public VeniceOfflinePushMonitorAccessor getVeniceOfflinePushMonitorAccessor() {
    return veniceOfflinePushMonitorAccessor;
  }

  public PushStatusStoreWriter getStatusStoreWriter() {
    return statusStoreWriter;
  }

  public ReadOnlyStoreRepository getHelixReadOnlyStoreRepository() {
    return helixReadOnlyStoreRepository;
  }

  @Override
  public void handleMessage(KillOfflinePushMessage message) {
    VeniceStoreVersionConfig storeConfig = veniceConfigLoader.getStoreConfig(message.getKafkaTopic());
    if (ingestionService.containsRunningConsumption(storeConfig)) {
      // push is failed, stop consumption.
      LOGGER.info(
          "Receive the message to kill consumption for topic: {}, msgId: {}",
          message.getKafkaTopic(),
          message.getMessageId());
      ingestionService.killConsumptionTask(storeConfig.getStoreVersionName());
      LOGGER.info("Killed Consumption for topic: {}, msgId: {}", message.getKafkaTopic(), message.getMessageId());
    } else {
      LOGGER.info("Ignore the kill message for topic: {}", message.getKafkaTopic());
    }
  }
}
