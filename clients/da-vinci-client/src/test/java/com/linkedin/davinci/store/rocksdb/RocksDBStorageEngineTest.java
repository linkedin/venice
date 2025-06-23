package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_EMIT_DUPLICATE_KEY_METRIC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineAccessor;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RocksDBStorageEngineTest extends AbstractStorageEngineTest<RocksDBStorageEngine> {
  private static final int PARTITION_ID = 0;
  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;
  private static final String storeName = Utils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final int versionNumber = 0;
  private static final String topicName = Version.composeKafkaTopic(storeName, versionNumber);
  private int testCount = 0;

  @Override
  public void createStorageEngineForTest() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(false);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(mockVersion);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);
    Properties properties = new Properties();
    properties.put(ROCKSDB_EMIT_DUPLICATE_KEY_METRIC, "true");
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mockReadOnlyStoreRepository);
    storeConfig = new VeniceStoreVersionConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = StorageEngineAccessor
        .getInnerStorageEngine(storageService.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null));
    createStoreForTest();
  }

  @BeforeClass
  public void setUp() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    storageService.dropStorePartition(storeConfig, PARTITION_ID);
    storageService.stop();
  }

  @AfterMethod
  public void testCounter() {
    this.testCount++;
  }

  /**
   * Some tests require a reset if other tests have run before them, as they are sensitive to contamination.
   *
   * Alternatively, we could make {@link #setUp()} have {@link org.testng.annotations.BeforeMethod} and
   * {@link #cleanUp()} have {@link AfterMethod}, though that makes the class take longer than the current approach.
   */
  private void reset() throws Exception {
    if (this.testCount > 0) {
      cleanUp();
      setUp();
    }
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testGetByKeyPrefixManyKeys() {
    super.testGetByKeyPrefixManyKeys();
  }

  @Test
  public void testGetByKeyPrefixMaxSignedByte() {
    super.testGetByKeyPrefixMaxSignedByte();
  }

  @Test
  public void testGetByKeyPrefixMaxUnsignedByte() {
    super.testGetByKeyPrefixMaxUnsignedByte();
  }

  @Test
  public void testGetByKeyPrefixByteOverflow() {
    super.testGetByKeyPrefixByteOverflow();
  }

  @Test
  public void testGetAndPutPartitionOffset() {
    StorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    OffsetRecord offsetRecord = new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer());

    int segment = 0;
    int sequence = 10;
    String kafkaUrl = "kafkaUrl";
    ProducerPartitionState ppState = createProducerPartitionState(segment, sequence);
    GUID guid = new GUID();
    offsetRecord.setRealtimeTopicProducerState(kafkaUrl, guid, ppState);
    offsetRecord.setCheckpointLocalVersionTopicOffset(666L);
    rocksDBStorageEngine.putPartitionOffset(PARTITION_ID, offsetRecord);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).get().getLocalVersionTopicOffset(), 666L);
    ProducerPartitionState ppStateFromRocksDB =
        rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).get().getRealTimeProducerState(kafkaUrl, guid);
    Assert.assertEquals(ppStateFromRocksDB.getSegmentNumber(), segment);
    Assert.assertEquals(ppStateFromRocksDB.getMessageSequenceNumber(), sequence);
    rocksDBStorageEngine.clearPartitionOffset(PARTITION_ID);
    Assert.assertEquals(rocksDBStorageEngine.getPartitionOffset(PARTITION_ID).isPresent(), false);
  }

  private ProducerPartitionState createProducerPartitionState(int segment, int sequence) {
    ProducerPartitionState ppState = new ProducerPartitionState();
    ppState.segmentNumber = segment;
    ppState.segmentStatus = SegmentStatus.IN_PROGRESS.getValue();
    ppState.messageSequenceNumber = sequence;
    ppState.messageTimestamp = System.currentTimeMillis();
    ppState.checksumType = CheckSumType.NONE.getValue();
    ppState.checksumState = ByteBuffer.allocate(0);
    ppState.aggregates = new HashMap<>();
    ppState.debugInfo = new HashMap<>();
    return ppState;
  }

  @Test
  public void testGetAndPutStoreVersionState() {
    StorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;

    // Create a StoreVersionState record
    StoreVersionState storeVersionStateRecord = new StoreVersionState();
    storeVersionStateRecord.sorted = true;

    rocksDBStorageEngine.putStoreVersionState(storeVersionStateRecord);
    Assert.assertEquals(rocksDBStorageEngine.getStoreVersionState(), storeVersionStateRecord);

    // If no store version state is present in this metadata partition, null should be returned.
    rocksDBStorageEngine.clearStoreVersionState();
    Assert.assertNull(rocksDBStorageEngine.getStoreVersionState());
  }

  @Test
  public void testIllegalPartitionIdInGetAndPutPartitionOffset() {
    StorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> rocksDBStorageEngine
            .putPartitionOffset(-1, new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer())));

    Assert.assertThrows(IllegalArgumentException.class, () -> rocksDBStorageEngine.getPartitionOffset(-1));
  }

  @Test
  public void testDelete() {
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
    StorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    Set<Integer> persistedPartitionIds = rocksDBStorageEngine.getPersistedPartitionIds();
    Assert.assertEquals(persistedPartitionIds.size(), 2);
    Assert.assertTrue(persistedPartitionIds.contains(PARTITION_ID));
    Assert.assertTrue(persistedPartitionIds.contains(METADATA_PARTITION_ID));
    Assert.assertEquals(2, rocksDBStorageEngine.getStats().getKeyCountEstimate());
    Assert.assertEquals(0, rocksDBStorageEngine.getStats().getDuplicateKeyCountEstimate());
  }

  @Test
  public void testGetInvalidKeys() {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPartitioning() throws Exception {
    reset();
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice() throws Exception {
    reset();
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice() throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition() throws Exception {
    super.testOperationsOnNonExistingPartition();
  }

  @Test
  public void testGetPersistedPartitionIds() {
    StorageEngine testStorageEngine = getTestStoreEngine();
    Assert.assertEquals(testStorageEngine.getType(), PersistenceType.ROCKS_DB);
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    Set<Integer> persistedPartitionIds = rocksDBStorageEngine.getPersistedPartitionIds();
    Assert.assertEquals(persistedPartitionIds.size(), 2);
    Assert.assertTrue(persistedPartitionIds.contains(PARTITION_ID));
    Assert.assertTrue(persistedPartitionIds.contains(METADATA_PARTITION_ID));
  }

  @Test
  public void testRocksDBStoragePartitionType() {
    // Verify that data partition is created as regular RocksDB partition, not a RMD-RocksDB Partition.
    Assert.assertFalse(
        testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof ReplicationMetadataRocksDBStoragePartition);
    Assert.assertTrue(testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof RocksDBStoragePartition);
    // Verify that metadata partition is created as regular RocksDB partition, not a RMD-RocksDB Partition.
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof ReplicationMetadataRocksDBStoragePartition);
    Assert.assertTrue(testStoreEngine.getMetadataPartition() instanceof RocksDBStoragePartition);
  }

  @Test
  public void testHasConflictPersistedStoreEngineConfig() {
    StorageEngine testStorageEngine = getTestStoreEngine();
    RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) testStorageEngine;
    RocksDBServerConfig rocksDBServerConfigMock = mock(RocksDBServerConfig.class);
    when(rocksDBServerConfigMock.getTransformerValueSchema()).thenReturn("not_null");
    when(rocksDBServerConfigMock.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    rocksDBStorageEngine.setRocksDBServerConfig(rocksDBServerConfigMock);

    boolean result = rocksDBStorageEngine.hasConflictPersistedStoreEngineConfig();

    Assert.assertTrue(result);

  }
}
