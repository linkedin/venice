package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * A common base class to provide setup and teardown routines to be used in venice ParentHelixAdmin related test cases.
 */
public class AbstractTestVeniceParentHelixAdmin {
  static final int TIMEOUT_IN_MS = 60 * Time.MS_PER_SECOND;
  static int KAFKA_REPLICA_FACTOR = 3;
  static final String PUSH_JOB_DETAILS_STORE_NAME = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
  static final int MAX_PARTITION_NUM = 1024;
  static final String TEST_SCHEMA =
      "{\"type\":\"record\", \"name\":\"ValueRecord\", \"fields\": [{\"name\":\"number\", " + "\"type\":\"int\"}]}";

  static final String clusterName = "test-cluster";
  static final String regionName = "test-region";
  static final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  static final String zkMetadataNodePath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
  static final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  static final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  TopicManager topicManager;
  VeniceHelixAdmin internalAdmin;
  VeniceControllerConfig config;
  ZkClient zkClient;
  VeniceWriter veniceWriter;
  VeniceParentHelixAdmin parentAdmin = null;
  HelixVeniceClusterResources resources;
  VeniceAdminStats adminStats;
  Store store;
  ParentHelixOfflinePushAccessor accessor;
  HelixReadOnlyStoreConfigRepository readOnlyStoreConfigRepository;
  Map<String, ControllerClient> controllerClients = new HashMap<>();
  ClusterLockManager clusterLockManager;
  StoragePersonaRepository personaRepository;

  public void setupInternalMocks() {
    topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>(Arrays.asList(topicName))).when(topicManager).listTopics();
    Map<String, Long> topicRetentions = new HashMap<>();
    topicRetentions.put(topicName, Long.MAX_VALUE);
    doReturn(topicRetentions).when(topicManager).getAllTopicRetentions();
    doReturn(true).when(topicManager).containsTopicAndAllPartitionsAreOnline(topicName);

    internalAdmin = mock(VeniceHelixAdmin.class);
    doReturn(topicManager).when(internalAdmin).getTopicManager();
    SchemaEntry mockEntry = new SchemaEntry(0, TEST_SCHEMA);
    doReturn(mockEntry).when(internalAdmin).getKeySchema(anyString(), anyString());

    zkClient = mock(ZkClient.class);
    doReturn(zkClient).when(internalAdmin).getZkClient();
    doReturn(new HelixAdapterSerializer()).when(internalAdmin).getAdapterSerializer();

    ExecutionIdAccessor executionIdAccessor = mock(ExecutionIdAccessor.class);
    doReturn(executionIdAccessor).when(internalAdmin).getExecutionIdAccessor();
    doReturn(0L).when(executionIdAccessor).getLastSucceededExecutionId(any());

    // Occasionally the startStoreMigrationMonitor will run and throw NPE's unless the internal
    // helix admin can proffer a set of StoreConfigRepo. So we set up mocks for this
    // that... do a funny thing to get it to leave us alone. This SHOULD be mocked to return a proper
    // list of store configs for the sake of correctness, but async scheduled threads can be the bane
    // of reliable unit tests. TODO: Return a real list of store configs in this mock.
    readOnlyStoreConfigRepository = mock(HelixReadOnlyStoreConfigRepository.class);
    doReturn(Collections.emptyList()).when(readOnlyStoreConfigRepository).getAllStoreConfigs();
    doReturn(readOnlyStoreConfigRepository).when(internalAdmin).getStoreConfigRepo();

    personaRepository = mock(StoragePersonaRepository.class);

    store = mock(Store.class);
    doReturn(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION).when(store).getOffLinePushStrategy();
    doReturn(false).when(store).isMigrating();
    doReturn(store).when(internalAdmin).checkPreConditionForAclOp(any(), any());

    HelixReadWriteStoreRepository storeRepository = mock(HelixReadWriteStoreRepository.class);
    doReturn(store).when(storeRepository).getStore(any());

    config = mockConfig(clusterName);
    doReturn(1).when(config).getReplicationMetadataVersion();

    controllerClients
        .put(regionName, ControllerClient.constructClusterControllerClient(clusterName, "localhost", Optional.empty()));
    doReturn(controllerClients).when(internalAdmin).getControllerClientMap(any());

    resources = mockResources(config, clusterName);
    doReturn(storeRepository).when(resources).getStoreMetadataRepository();
    ZkRoutersClusterManager manager = mock(ZkRoutersClusterManager.class);
    doReturn(manager).when(resources).getRoutersClusterManager();
    ZkStoreConfigAccessor storeConfigAccessor = mock(ZkStoreConfigAccessor.class);
    doReturn(false).when(storeConfigAccessor).containsConfig(anyString());
    doReturn(storeConfigAccessor).when(resources).getStoreConfigAccessor();
    doReturn(personaRepository).when(resources).getStoragePersonaRepository();

    doReturn(10).when(manager).getLiveRoutersCount();
    clusterLockManager = mock(ClusterLockManager.class);
    doReturn(clusterLockManager).when(resources).getClusterLockManager();

    adminStats = mock(VeniceAdminStats.class);
    doReturn(adminStats).when(resources).getVeniceAdminStats();

    accessor = mock(ParentHelixOfflinePushAccessor.class);

    // Need to bypass VeniceWriter initialization
    veniceWriter = mock(VeniceWriter.class);
  }

  /**
   * Separate internal mocks setup and initialization so tests can change the behavior of the mocks without running into
   * concurrency issues. i.e. change mock's behavior in test thread while it's being used in some background threads.
   */
  public void initializeParentAdmin(Optional<AuthorizerService> authorizerService) {
    parentAdmin = new VeniceParentHelixAdmin(
        internalAdmin,
        TestUtils.getMultiClusterConfigFromOneCluster(config),
        false,
        Optional.empty(),
        authorizerService);
    ControllerClient mockControllerClient = mock(ControllerClient.class);
    doReturn(new ControllerResponse()).when(mockControllerClient).checkResourceCleanupForStoreCreation(anyString());
    doReturn(new StoreResponse()).when(mockControllerClient).getStore(anyString());
    parentAdmin.getAdminCommandExecutionTracker(clusterName)
        .get()
        .getFabricToControllerClientsMap()
        .put(regionName, mockControllerClient);
    parentAdmin.setOfflinePushAccessor(accessor);
    parentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
  }

  public void cleanupTestCase() {
    controllerClients.values().forEach(ControllerClient::close);
    controllerClients.clear();
    if (parentAdmin != null) {
      parentAdmin.close();
    }
  }

  VeniceControllerConfig mockConfig(String clusterName) {
    VeniceControllerConfig config = mock(VeniceControllerConfig.class);
    doReturn(clusterName).when(config).getClusterName();
    doReturn(KAFKA_REPLICA_FACTOR).when(config).getKafkaReplicationFactor();
    doReturn(KAFKA_REPLICA_FACTOR).when(config).getAdminTopicReplicationFactor();
    doReturn(10000).when(config).getParentControllerWaitingTimeForConsumptionMs();
    doReturn("fake_kafka_bootstrap_servers").when(config).getKafkaBootstrapServers();
    // PushJobStatusStore and participant message store are disabled in this unit test by default because many
    // tests are using verify(veniceWriter).put(...) which could be unpredictable with async setup enabled.
    doReturn("").when(config).getPushJobStatusStoreClusterName();
    doReturn(false).when(config).isParticipantMessageStoreEnabled();
    // Disable background threads that may interfere when we try to re-mock internalAdmin later in the tests.
    doReturn(Long.MAX_VALUE).when(config).getTerminalStateTopicCheckerDelayMs();
    Map<String, String> childClusterMap = new HashMap<>();
    childClusterMap.put(regionName, "localhost");
    doReturn(childClusterMap).when(config).getChildDataCenterControllerUrlMap();
    doReturn(MAX_PARTITION_NUM).when(config).getMaxNumberOfPartition();
    doReturn(DefaultIdentityParser.class.getName()).when(config).getIdentityParserClassName();
    return config;
  }

  HelixVeniceClusterResources mockResources(VeniceControllerConfig config, String clusterName) {
    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    doReturn(config).when(resources).getConfig();
    doReturn(resources).when(internalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(clusterLockManager).when(resources).getClusterLockManager();
    return resources;
  }

}
