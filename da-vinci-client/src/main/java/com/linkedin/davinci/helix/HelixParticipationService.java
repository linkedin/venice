package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.DefaultIngestionBackend;
import com.linkedin.davinci.ingestion.IsolatedIngestionBackend;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.davinci.notifier.PartitionPushStatusNotifier;
import com.linkedin.davinci.notifier.PushMonitorNotifier;
import com.linkedin.davinci.stats.ParticipantStateStats;
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

import static com.linkedin.venice.ConfigKeys.*;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService implements StatusMessageHandler<KillOfflinePushMessage> {

  private static final Logger logger = LogManager.getLogger(HelixParticipationService.class);

  private static final String ONLINE_OFFLINE_MODEL_NAME = "PartitionOnlineOfflineModel";

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
  private final CompletableFuture<SafeHelixManager> managerFuture; //complete this future when the manager is connected
  private final CompletableFuture<HelixPartitionStatusAccessor> partitionPushStatusAccessorFuture;

  private ZkClient zkClient;
  private SafeHelixManager helixManager;
  private AbstractStateModelFactory onlineOfflineParticipantModelFactory;
  private AbstractStateModelFactory leaderFollowerParticipantModelFactory;
  private HelixPartitionStatusAccessor partitionPushStatusAccessor;

  public HelixParticipationService(StoreIngestionService storeIngestionService, StorageService storageService,
      StorageMetadataService storageMetadataService, VeniceConfigLoader veniceConfigLoader,
      ReadOnlyStoreRepository helixReadOnlyStoreRepository, MetricsRepository metricsRepository, String zkAddress,
      String clusterName, int port, CompletableFuture<SafeHelixManager> managerFuture) {
    this.ingestionService = storeIngestionService;
    this.storageService = storageService;
    this.clusterName = clusterName;
    //The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    this.participantName = Utils.getHelixNodeIdentifier(port);
    this.zkAddress = zkAddress;
    this.veniceConfigLoader = veniceConfigLoader;
    this.helixReadOnlyStoreRepository = helixReadOnlyStoreRepository;
    this.metricsRepository = metricsRepository;
    this.instance = new Instance(participantName, Utils.getHostName(), port);
    this.managerFuture = managerFuture;
    this.partitionPushStatusAccessorFuture = new CompletableFuture<>();
    if (!(storeIngestionService instanceof KafkaStoreIngestionService)) {
      throw new VeniceException("Expecting " + KafkaStoreIngestionService.class.getName() + " for ingestion backend!");
    }
    if (veniceConfigLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED)) {
      this.ingestionBackend = new IsolatedIngestionBackend(veniceConfigLoader, helixReadOnlyStoreRepository, metricsRepository, storageMetadataService, (KafkaStoreIngestionService) storeIngestionService, storageService);
    } else {
      this.ingestionBackend = new DefaultIngestionBackend(storageMetadataService, (KafkaStoreIngestionService) storeIngestionService, storageService);
    }
    new ParticipantStateStats(metricsRepository, "venice_O/O_partition_state");
  }

  //Set corePoolSize and maxPoolSize as the same value, but enable allowCoreThreadTimeOut. So the expected
  //behavior is pool will create a new thread if the number of running threads is fewer than corePoolSize, otherwise
  //add this task into queue. If a thread is idle for more than 300 seconds, pool will collect this thread.
  private ThreadPoolExecutor initHelixStateTransitionThreadPool(int size, String threadName,
      MetricsRepository metricsRepository, String statsName) {
    ThreadPoolExecutor helixStateTransitionThreadPool = new ThreadPoolExecutor(size, size, 300L,
        TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new DaemonThreadFactory(threadName));
    helixStateTransitionThreadPool.allowCoreThreadTimeOut(true);

    //register stats that tracks the thread pool
    new ThreadPoolStats(metricsRepository, helixStateTransitionThreadPool, statsName);

    return helixStateTransitionThreadPool;
  }

  @Override
  public boolean startInner() {
    logger.info("Attempting to start HelixParticipation service");
    helixManager = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, this.participantName, InstanceType.PARTICIPANT, zkAddress));

    //create 2 dedicated thread pools for executing incoming state transitions (1 for online offline (O/O) model and the
    //other for leader follower (L/F) model) Since L/F transition is not blocked by ingestion, it should run much faster
    //than O/O's. Thus, the size is supposed to be smaller.
    onlineOfflineParticipantModelFactory = new OnlineOfflinePartitionStateModelFactory(
        ingestionBackend,
        veniceConfigLoader,
        initHelixStateTransitionThreadPool(
            veniceConfigLoader.getVeniceServerConfig().getMaxOnlineOfflineStateTransitionThreadNumber(),
            "venice-O/O-state-transition",
            metricsRepository,
            "Venice_ST_thread_pool"),
        helixReadOnlyStoreRepository, partitionPushStatusAccessorFuture, instance.getNodeId());

    leaderFollowerParticipantModelFactory = new LeaderFollowerPartitionStateModelFactory(
        ingestionBackend,
        veniceConfigLoader,
        initHelixStateTransitionThreadPool(
            veniceConfigLoader.getVeniceServerConfig().getMaxLeaderFollowerStateTransitionThreadNumber(),
            "venice-L/F-state-transition",
            metricsRepository,
            "Venice_L/F_ST_thread_pool"),
        helixReadOnlyStoreRepository, partitionPushStatusAccessorFuture, instance.getNodeId());

    helixManager.getStateMachineEngine().registerStateModelFactory(ONLINE_OFFLINE_MODEL_NAME, onlineOfflineParticipantModelFactory);
    helixManager.getStateMachineEngine().registerStateModelFactory(LeaderStandbySMD.name, leaderFollowerParticipantModelFactory);
    //TODO Now Helix instance config only support host and port. After talking to Helix team, they will add
    // customize k-v data support soon. Then we don't need LiveInstanceInfoProvider here. Use the instance config
    // is a better way because it reduce the communication times to Helix. Other wise client need to get  thsi
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

    //TODO Venice Listener should not be started, until the HelixService is started.
    asyncStart();

    // The start up process may not be finished yet, because it is continuing asynchronously.
    return false;
  }

  @Override
  public void stopInner() throws IOException {
    logger.info("Attempting to stop HelixParticipation service.");
    if (helixManager != null) {
      try {
        helixManager.disconnect();
        logger.info("Disconnected Helix Manager.");
      } catch (Exception e) {
        logger.error("Swallowed an exception while trying to disconnect the " + helixManager.getClass().getSimpleName(), e);
      }
    } else {
      logger.info("Helix Manager is null.");
    }
    ingestionBackend.close();
    logger.info("Closed VeniceIngestionBackend.");
    onlineOfflineParticipantModelFactory.getExecutorService().shutdownNow();
    leaderFollowerParticipantModelFactory.getExecutorService().shutdownNow();
    try {
      onlineOfflineParticipantModelFactory.getExecutorService().awaitTermination(30, TimeUnit.SECONDS);
      leaderFollowerParticipantModelFactory.getExecutorService().awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (zkClient != null) {
      logger.info("Start closing ZkClient.");
      zkClient.close();
      logger.info("Closed ZkClient.");
    }
    logger.info("Finished stopping HelixParticipation service.");
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
    VeniceWriterFactory writerFactory =new VeniceWriterFactory(veniceProperties.toProperties());
    PushStatusStoreWriter statusStoreWriter = new PushStatusStoreWriter(
        writerFactory, instance.getNodeId(), veniceProperties.getInt(PUSH_STATUS_STORE_DERIVED_SCHEMA_ID, 1));

    // Record replica status in Zookeeper.
    // Need to be started before connecting to ZK, otherwise some notification will not be sent by this notifier.
    PushMonitorNotifier pushMonitorNotifier = new PushMonitorNotifier(
        new VeniceOfflinePushMonitorAccessor(clusterName, zkClient, new HelixAdapterSerializer(),
            veniceConfigLoader.getVeniceClusterConfig().getRefreshAttemptsForZkReconnect(),
            veniceConfigLoader.getVeniceClusterConfig().getRefreshIntervalForZkReconnectInMs()),
        statusStoreWriter,
        helixReadOnlyStoreRepository,
        instance.getNodeId());

    ingestionBackend.addPushStatusNotifier(pushMonitorNotifier);

    CompletableFuture.runAsync(() -> {
      try {
        // Check node status before joining the cluster.
        // TODO Helix team will provide a way to let us do some checking after connecting to Helix.
        // TODO In that case, it's guaranteed that the node would not be assigned with any resource before we completed our
        // TODO checking, so we could use HelixManager to get some metadata instead of creating a new zk connection.
        checkBeforeJoinInCluster();
        helixManager.connect();
        managerFuture.complete(helixManager);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        logger.error("Venice server is about to close");
        Utils.exit("Failed to start HelixParticipationService");
      }

      /**
       * The accessor can only get created successfully after helix manager is created.
       */
      partitionPushStatusAccessor =
          new HelixPartitionStatusAccessor(helixManager.getOriginalManager(), instance.getNodeId(),
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
      logger.info("Successfully started Helix partition status accessor.");

      serviceState.set(ServiceState.STARTED);
      logger.info("Successfully started Helix Participation Service.");
    });
  }

  @Override
  public void handleMessage(KillOfflinePushMessage message) {
    VeniceStoreVersionConfig storeConfig = veniceConfigLoader.getStoreConfig(message.getKafkaTopic());
    if (ingestionService.containsRunningConsumption(storeConfig)) {
      //push is failed, stop consumption.
      logger.info("Receive the message to kill consumption for topic:" + message.getKafkaTopic() + ", msgId: " + message
          .getMessageId());
      ingestionService.killConsumptionTask(storeConfig.getStoreVersionName());
      logger.info("Killed Consumption for topic:" + message.getKafkaTopic() + ", msgId: " + message.getMessageId());
    } else {
      logger.info("Ignore the kill message for topic:" + message.getKafkaTopic());
    }
  }
}
