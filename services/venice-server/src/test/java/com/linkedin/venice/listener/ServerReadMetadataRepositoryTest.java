package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.schema.SchemaEntry;
import io.tehuti.metrics.MetricsRepository;
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
  private final MetricsRepository metricsRepository = new MetricsRepository();
  private final static String TEST_STORE = "test_store";

  @BeforeMethod
  public void setUp() {
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    mockHelixInstanceConfigRepository = mock(HelixInstanceConfigRepository.class);
  }

  @Test
  public void testGetMetadata() {
    ServerReadMetadataRepository serverReadMetadataRepository = new ServerReadMetadataRepository(
        metricsRepository,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.of(CompletableFuture.completedFuture(mockCustomizedViewRepository)),
        Optional.of(CompletableFuture.completedFuture(mockHelixInstanceConfigRepository)));
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
    doReturn(0).when(partition).getId();
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
  public void storeMigrationShouldThrownException() {
    ServerReadMetadataRepository serverReadMetadataRepository = new ServerReadMetadataRepository(
        metricsRepository,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.of(CompletableFuture.completedFuture(mockCustomizedViewRepository)),
        Optional.of(CompletableFuture.completedFuture(mockHelixInstanceConfigRepository)));

    Store store = mock(Store.class);
    doReturn(Boolean.TRUE).when(store).isMigrating();
    doReturn(Boolean.TRUE).when(store).isStorageNodeReadQuotaEnabled();
    doReturn(store).when(mockMetadataRepo).getStoreOrThrow(TEST_STORE);
    MetadataResponse response = serverReadMetadataRepository.getMetadata(TEST_STORE);
    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getMessage().contains(TEST_STORE + " is migrating"));
  }
}
