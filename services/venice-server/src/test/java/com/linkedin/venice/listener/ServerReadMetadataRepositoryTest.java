package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.StorePropertiesPayload;
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
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
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
  private static final String TEST_STORE = "test_store";
  private static final String DEST_CLUSTER = "test-cluster-dst";
  private static final String SRC_CLUSTER = "test-cluster-src";
  private static final String TEHUTI_INVOKE_METRIC = ".ServerMetadataStats--request_based_metadata_invoke_count.Rate";
  private static final String TEHUTI_FAILURE_METRIC = ".ServerMetadataStats--request_based_metadata_failure_count.Rate";

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

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
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

    // When storage node read quota is not enabled, UOE is thrown (caught by catch(Exception), metric recorded,
    // re-thrown)
    assertThrows(UnsupportedOperationException.class, () -> serverReadMetadataRepository.getMetadata(storeName));
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);

    mockStore.setStorageNodeReadQuotaEnabled(true);
    MetadataResponse metadataResponse = serverReadMetadataRepository.getMetadata(storeName);
    assertNotNull(metadataResponse);
    assertEquals(metadataResponse.getResponseRecord().getKeySchema().get("0"), "\"string\"");
    // Verify the metadata
    assertEquals(metadataResponse.getResponseRecord().getVersions().size(), 2);
    VersionProperties versionProperties = metadataResponse.getResponseRecord().getVersionMetadata();
    assertNotNull(versionProperties);
    assertEquals(versionProperties.getCurrentVersion(), 2);
    assertEquals(versionProperties.getPartitionCount(), 1);
    assertEquals(metadataResponse.getResponseRecord().getRoutingInfo().get("0").size(), 1);
    // If batch get limit is not set should use {@link Store.DEFAULT_BATCH_GET_LIMIT}
    assertEquals(metadataResponse.getResponseRecord().getBatchGetLimit(), Store.DEFAULT_BATCH_GET_LIMIT);
    assertTrue(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value() > 0);
    // Failure count is non-zero because the UnsupportedOperationException path (quota not enabled) records a failure
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);

    ServerCurrentVersionResponse currentVersionResponse =
        serverReadMetadataRepository.getCurrentVersionResponse(storeName);
    assertNotNull(currentVersionResponse);
    assertEquals(currentVersionResponse.getCurrentVersion(), 2);

    mockStore.setBatchGetLimit(300);
    metadataResponse = serverReadMetadataRepository.getMetadata(storeName);
    assertEquals(metadataResponse.getResponseRecord().getBatchGetLimit(), 300);
  }

  @Test
  public void testGetStoreProperties() {
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
    ArrayList<SchemaEntry> valueSchemas = new ArrayList<>();
    final int schemaCount = 3;
    for (int i = 1; i <= schemaCount; i++) {
      valueSchemas.add(new SchemaEntry(i, schema));
    }
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    Mockito.when(mockSchemaRepo.getKeySchema(storeName)).thenReturn(new SchemaEntry(0, schema));
    Mockito.when(mockSchemaRepo.getValueSchemas(storeName)).thenReturn(valueSchemas);
    Mockito.when(mockCustomizedViewRepository.getPartitionAssignments(topicName)).thenReturn(partitionAssignment);
    Mockito.when(mockHelixInstanceConfigRepository.getInstanceGroupIdMapping()).thenReturn(Collections.emptyMap());
    mockStore.setStorageNodeReadQuotaEnabled(true);

    // Request
    StorePropertiesPayload storePropertiesPayload =
        serverReadMetadataRepository.getStoreProperties(storeName, Optional.empty());
    StoreMetaValue storeMetaValue =
        deserializeStoreMetaValue(storePropertiesPayload.getPayloadRecord().getStoreMetaValueAvro().array());
    assertNotNull(storePropertiesPayload);
    assertNotNull(storePropertiesPayload.getPayloadRecord());
    assertNotNull(storePropertiesPayload.getPayloadRecord().getStoreMetaValueAvro());

    // Assert response
    assertEquals(storeMetaValue.getStoreKeySchemas().getKeySchemaMap().get(new Utf8("0")), new Utf8("\"string\""));
    assertEquals(storeMetaValue.getStoreProperties().getVersions().size(), 2);
    assertNotNull(storePropertiesPayload.getPayloadRecord().getRoutingInfo());
    assertNotNull(storePropertiesPayload.getPayloadRecord().getRoutingInfo().get("0"));
    assertEquals(storePropertiesPayload.getPayloadRecord().getRoutingInfo().get("0").size(), 1);
    ServerCurrentVersionResponse currentVersionResponse =
        serverReadMetadataRepository.getCurrentVersionResponse(storeName);
    assertNotNull(currentVersionResponse);
    assertEquals(currentVersionResponse.getCurrentVersion(), 2);

    // Assert metrics repo
    assertTrue(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value() > 0);
    assertEquals(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value(), 0d);

    // Test largestKnownSchemaID param
    for (int i = 0; i <= schemaCount; i++) {
      StorePropertiesPayload storePropertiesPayloadLKSID =
          serverReadMetadataRepository.getStoreProperties(storeName, Optional.of(i));
      StoreMetaValue storeMetaValueLKSID =
          deserializeStoreMetaValue(storePropertiesPayloadLKSID.getPayloadRecord().getStoreMetaValueAvro().array());
      assertEquals(storeMetaValueLKSID.getStoreValueSchemas().getValueSchemaMap().size(), schemaCount - i);
    }

    // Value update test
    mockStore.setBatchGetLimit(300);
    StorePropertiesPayload storePropertiesPayloadValueUpdate =
        serverReadMetadataRepository.getStoreProperties(storeName, Optional.empty());
    StoreMetaValue storeMetaValueUpdate =
        deserializeStoreMetaValue(storePropertiesPayloadValueUpdate.getPayloadRecord().getStoreMetaValueAvro().array());
    assertEquals(storeMetaValueUpdate.getStoreProperties().getBatchGetLimit(), 300);
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
    assertFalse(response.isError());
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
    assertTrue(response.isError());
    assertTrue(response.getMessage().contains(TEST_STORE + " is migrating"));
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
    assertTrue(response.isError());
    assertTrue(response.getMessage().contains(TEST_STORE + " does not exist"));
  }

  @Test
  public void testMetadataUrlSchemeRespectsSSLSetting() {
    // Create a non-SSL repository
    ServerReadMetadataRepository nonSslRepo = new ServerReadMetadataRepository(
        SRC_CLUSTER,
        new MetricsRepository(),
        mockMetadataRepo,
        mockSchemaRepo,
        storeConfigRepository,
        Optional.of(CompletableFuture.completedFuture(mockCustomizedViewRepository)),
        Optional.of(CompletableFuture.completedFuture(mockHelixInstanceConfigRepository)),
        false);

    String storeName = "test-store-ssl";
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
    mockStore.setCurrentVersion(1);
    mockStore.setStorageNodeReadQuotaEnabled(true);
    String topicName = Version.composeKafkaTopic(storeName, 1);
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

    // Non-SSL repo should generate http:// URLs in metadata routing info
    MetadataResponse nonSslResponse = nonSslRepo.getMetadata(storeName);
    assertFalse(nonSslResponse.isError());
    // Routing info values are lists of instance URLs built via instance.getUrl(sslEnabled)
    // With sslEnabled=false, URLs should start with "http://"
    CharSequence nonSslUrl = nonSslResponse.getResponseRecord().getRoutingInfo().get("0").get(0);
    assertTrue(nonSslUrl.toString().startsWith("http://"), "Expected http:// URL but got: " + nonSslUrl);
    assertFalse(nonSslUrl.toString().startsWith("https://"), "Should not be https:// but got: " + nonSslUrl);

    // Default (SSL) repo should generate https:// URLs
    MetadataResponse sslResponse = serverReadMetadataRepository.getMetadata(storeName);
    assertFalse(sslResponse.isError());
    CharSequence sslUrl = sslResponse.getResponseRecord().getRoutingInfo().get("0").get(0);
    assertTrue(sslUrl.toString().startsWith("https://"), "Expected https:// URL but got: " + sslUrl);
  }

  @Test
  public void testGetStorePropertiesRecordsFailureOnVeniceNoStoreException() {
    String storeName = "nonexistent-store";
    doThrow(new VeniceNoStoreException(storeName)).when(mockMetadataRepo).getStoreOrThrow(storeName);
    StorePropertiesPayload response = serverReadMetadataRepository.getStoreProperties(storeName, Optional.empty());
    assertStorePropertiesFailure(response, storeName);
  }

  @Test
  public void testGetStorePropertiesRecordsFailureOnGenericException() {
    String storeName = "bad-store";
    doThrow(new RuntimeException("unexpected error")).when(mockMetadataRepo).getStoreOrThrow(storeName);
    StorePropertiesPayload response = serverReadMetadataRepository.getStoreProperties(storeName, Optional.empty());
    assertStorePropertiesFailure(response, "unexpected error");
  }

  @Test
  public void testGetMetadataRecordsFailureOnVeniceNoStoreException() {
    String storeName = "nonexistent-metadata-store";
    doThrow(new VeniceNoStoreException(storeName)).when(mockMetadataRepo).getStoreOrThrow(storeName);

    MetadataResponse response = serverReadMetadataRepository.getMetadata(storeName);
    assertTrue(response.isError());
    assertTrue(response.getMessage().contains(storeName));
    // Tehuti failure sensor recorded (via no-arg overload), invoke sensor also recorded
    assertTrue(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value() > 0);
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
  }

  @Test
  public void testGetMetadataRecordsFailureOnGenericException() {
    String storeName = "bad-metadata-store";
    // RuntimeException is not a VeniceException, so it's caught by catch(Exception e), recorded, and re-thrown
    doThrow(new RuntimeException("unexpected error")).when(mockMetadataRepo).getStoreOrThrow(storeName);

    assertThrows(RuntimeException.class, () -> serverReadMetadataRepository.getMetadata(storeName));
    assertTrue(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value() > 0);
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
  }

  private void assertStorePropertiesFailure(StorePropertiesPayload response, String expectedMessageSubstring) {
    assertTrue(response.isError());
    assertTrue(response.getMessage().contains(expectedMessageSubstring));
    assertTrue(metricsRepository.getMetric(TEHUTI_INVOKE_METRIC).value() > 0);
    assertTrue(metricsRepository.getMetric(TEHUTI_FAILURE_METRIC).value() > 0);
  }

  private StoreMetaValue deserializeStoreMetaValue(byte[] bytes) {
    Schema schema = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
    RecordDeserializer<StoreMetaValue> storeMetaValueRecordDeserializer =
        FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(schema, StoreMetaValue.class);
    return storeMetaValueRecordDeserializer.deserialize(bytes);
  }
}
