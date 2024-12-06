package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.schema.SchemaEntry;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerReadMetadataRepositoryTest {
  private ReadOnlyStoreRepository mockMetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private HelixCustomizedViewOfflinePushRepository mockCustomizedViewRepository;
  private HelixInstanceConfigRepository mockHelixInstanceConfigRepository;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;
  private ServerReadMetadataRepository serverReadMetadataRepository;
  private MetricsRepository metricsRepository;
  private final static String TEST_STORE = "test_store";
  private final static String DEST_CLUSTER = "test-cluster-dst";
  private final static String SRC_CLUSTER = "test-cluster-src";

  @BeforeMethod
  public void setUp() {
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    mockHelixInstanceConfigRepository = mock(HelixInstanceConfigRepository.class);
    storeConfigRepository = mock(HelixReadOnlyStoreConfigRepository.class);
    metricsRepository = new MetricsRepository();
    serverReadMetadataRepository = new ServerReadMetadataRepository(
        SRC_CLUSTER,
        metricsRepository,
        mockMetadataRepo,
        mockSchemaRepo,
        storeConfigRepository,
        Optional.of(CompletableFuture.completedFuture(mockCustomizedViewRepository)),
        Optional.of(CompletableFuture.completedFuture(mockHelixInstanceConfigRepository)));
  }

  @Test
  public void testGetMetadata() {
    String storeName = "test-store";
    Store mockStore = new ZKStore(
        storeName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    mockStore.addVersion(new VersionImpl(storeName, 2, "test-job-id2"));
    mockStore.setCurrentVersion(2);
    mockStore.setStorageNodeReadQuotaEnabled(false);
    String topicName = Version.composeKafkaTopic(storeName, 2);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1);
    Partition partition = mock(Partition.class);
    when(partition.getId()).thenReturn(0);
    List<Instance> readyToServeInstances = Collections.singletonList(new Instance("host1", "host1", 1234));
    doReturn(readyToServeInstances).when(partition).getReadyToServeInstances();
    partitionAssignment.addPartition(partition);

    String schema = "\"string\"";
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    Mockito.when(mockSchemaRepo.getKeySchema(storeName)).thenReturn(new SchemaEntry(0, schema));
    Mockito.when(mockSchemaRepo.getValueSchemas(storeName))
        .thenReturn(Collections.singletonList(new SchemaEntry(0, schema)));
    Mockito.when(mockCustomizedViewRepository.getPartitionAssignments(topicName)).thenReturn(partitionAssignment);
    Mockito.when(mockHelixInstanceConfigRepository.getInstanceGroupIdMapping()).thenReturn(Collections.emptyMap());

    Assert.assertThrows(UnsupportedOperationException.class, () -> serverReadMetadataRepository.getMetadata(storeName));
    mockStore.setStorageNodeReadQuotaEnabled(true);
    MetadataResponse metadataResponse = serverReadMetadataRepository.getMetadata(storeName);
    Assert.assertNotNull(metadataResponse);
    Assert.assertEquals(metadataResponse.getResponseRecord().getKeySchema().get("0"), "\"string\"");
    // Verify the metadata
    Assert.assertEquals(metadataResponse.getResponseRecord().getVersions().size(), 2);
    VersionProperties versionProperties = metadataResponse.getResponseRecord().getVersionMetadata();
    Assert.assertNotNull(versionProperties);
    Assert.assertEquals(versionProperties.getCurrentVersion(), 2);
    Assert.assertEquals(versionProperties.getPartitionCount(), 1);
    Assert.assertEquals(metadataResponse.getResponseRecord().getRoutingInfo().get("0").size(), 1);
    // If batch get limit is not set should use {@link Store.DEFAULT_BATCH_GET_LIMIT}
    Assert.assertEquals(metadataResponse.getResponseRecord().getBatchGetLimit(), Store.DEFAULT_BATCH_GET_LIMIT);
    String metadataInvokeMetricName = ".ServerMetadataStats--request_based_metadata_invoke_count.Rate";
    String metadataFailureMetricName = ".ServerMetadataStats--request_based_metadata_failure_count.Rate";
    Assert.assertTrue(metricsRepository.getMetric(metadataInvokeMetricName).value() > 0);
    Assert.assertEquals(metricsRepository.getMetric(metadataFailureMetricName).value(), 0d);

    ServerCurrentVersionResponse currentVersionResponse =
        serverReadMetadataRepository.getCurrentVersionResponse(storeName);
    Assert.assertNotNull(currentVersionResponse);
    Assert.assertEquals(currentVersionResponse.getCurrentVersion(), 2);

    mockStore.setBatchGetLimit(300);
    metadataResponse = serverReadMetadataRepository.getMetadata(storeName);
    Assert.assertEquals(metadataResponse.getResponseRecord().getBatchGetLimit(), 300);
  }

  @Test
  public void storeMigrationShouldNotThrownExceptionWhenStartMigration() {
    Store store = mock(Store.class);
    String topicName = Version.composeKafkaTopic(TEST_STORE, 1);
    doReturn(Boolean.TRUE).when(store).isMigrating();
    doReturn(Boolean.TRUE).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(store).when(mockMetadataRepo).getStoreOrThrow(TEST_STORE);
    StoreConfig storeConfig = new StoreConfig(TEST_STORE);
    storeConfig.setMigrationDestCluster(DEST_CLUSTER);
    storeConfig.setMigrationSrcCluster(SRC_CLUSTER);
    storeConfig.setCluster(SRC_CLUSTER);
    doReturn(storeConfig).when(storeConfigRepository).getStoreConfigOrThrow(TEST_STORE);
    doReturn(1).when(store).getCurrentVersion();
    Version version = mock(Version.class);
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    CompressionStrategy strategy = CompressionStrategy.NO_OP;
    doReturn(strategy).when(version).getCompressionStrategy();
    doReturn(partitionerConfig).when(version).getPartitionerConfig();
    doReturn(version).when(store).getVersionOrThrow(anyInt());
    String schema = "\"string\"";
    SchemaEntry entry = new SchemaEntry(0, schema);
    List<SchemaEntry> schemas = new ArrayList<>();
    schemas.add(entry);
    doReturn(entry).when(mockSchemaRepo).getKeySchema(TEST_STORE);
    doReturn(schemas).when(mockSchemaRepo).getValueSchemas(TEST_STORE);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1);
    Partition partition = mock(Partition.class);
    when(partition.getId()).thenReturn(0);
    List<Instance> readyToServeInstances = Collections.singletonList(new Instance("host1", "host1", 1234));
    doReturn(readyToServeInstances).when(partition).getReadyToServeInstances();
    partitionAssignment.addPartition(partition);
    when(mockCustomizedViewRepository.getPartitionAssignments(topicName)).thenReturn(partitionAssignment);
    when(mockHelixInstanceConfigRepository.getInstanceGroupIdMapping()).thenReturn(Collections.emptyMap());

    MetadataResponse response = serverReadMetadataRepository.getMetadata(TEST_STORE);
    Assert.assertFalse(response.isError());
  }

  @Test
  public void storeMigrationShouldThrownExceptionWhenMigrationCompletes() {
    Store store = mock(Store.class);
    doReturn(Boolean.TRUE).when(store).isMigrating();
    doReturn(Boolean.TRUE).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(store).when(mockMetadataRepo).getStoreOrThrow(TEST_STORE);
    StoreConfig storeConfig = new StoreConfig(TEST_STORE);
    storeConfig.setCluster(DEST_CLUSTER);
    doReturn(storeConfig).when(storeConfigRepository).getStoreConfigOrThrow(TEST_STORE);

    MetadataResponse response = serverReadMetadataRepository.getMetadata(TEST_STORE);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getMessage().contains(TEST_STORE + " is migrating"));
  }

  @Test
  public void storeMigrationShouldThrownExceptionWhenStoreConfigMisfunction() {
    Store store = mock(Store.class);
    doReturn(Boolean.TRUE).when(store).isMigrating();
    doReturn(Boolean.TRUE).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(store).when(mockMetadataRepo).getStoreOrThrow(TEST_STORE);
    doThrow(new VeniceNoStoreException(TEST_STORE)).when(storeConfigRepository).getStoreConfigOrThrow(TEST_STORE);

    // store config is not available
    MetadataResponse response = serverReadMetadataRepository.getMetadata(TEST_STORE);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getMessage().contains(TEST_STORE + " does not exist"));
  }
}
