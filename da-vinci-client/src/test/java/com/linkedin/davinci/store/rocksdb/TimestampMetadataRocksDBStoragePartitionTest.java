package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TimestampMetadataRocksDBStoragePartitionTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private final String storeName = TestUtils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private final int versionNumber = 0;
  private final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  private static final String DATA_BASE_DIR = TestUtils.getUniqueTempPath();
  private static final String keyPrefix = "key_";
  private static final String valuePrefix = "value_";
  private static final String metadataPrefix = "metadata_";
  private static final RocksDBThrottler rocksDbThrottler = new RocksDBThrottler(3);

  private StorageService storageService;
  private VeniceStoreConfig storeConfig;

  private Map<String, Pair<String, String>> generateInputWithMetadata(int recordCnt) {
    return generateInputWithMetadata(0, recordCnt);
  }

  private Map<String, Pair<String, String>> generateInputWithMetadata(int startIndex, int endIndex) {
    Map<String, Pair<String, String>> records = new HashMap<>();
    for (int i = startIndex; i < endIndex; ++i) {
      String value = valuePrefix + i;
      String metadata = metadataPrefix + i;
      records.put(keyPrefix + i, Pair.create(value, metadata));
    }
    return records;
  }

  private String getTempDatabaseDir(String storeName) {
    File storeDir = new File(DATA_BASE_DIR, storeName).getAbsoluteFile();
    if (!storeDir.mkdirs()) {
      throw new VeniceException("Failed to mkdirs for path: " + storeDir.getPath());
    }
    storeDir.deleteOnExit();
    return storeDir.getPath();
  }

  private void removeDir(String path) {
    File file = new File(path);
    if (file.exists() && !file.delete()) {
      throw new VeniceException("Failed to remove path: " + path);
    }
  }

  @Override
  public void createStorageEngineForTest() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(true);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(Optional.of(mockVersion));
    when(mockReadOnlyStoreRepository.hasStore(storeName)).thenReturn(true);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mockReadOnlyStoreRepository);
    storeConfig = new VeniceStoreConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
  }

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() throws Exception {
    storageService.dropStorePartition(storeConfig , PARTITION_ID);
    storageService.stop();
  }
  @Test
  public void testUseTimestampMetadataRocksDBStoragePartition() {
    // Verify that data partition is created as TSMD-RocksDB Partition.
    Assert.assertTrue(testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof TimestampMetadataRocksDBStoragePartition);
    // Verify that metadata partition is not create as TSMD-RocksDB Partition
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof TimestampMetadataRocksDBStoragePartition);
  }

  @Test
  public void testMetadataColumnFamily() {
    String storeName = "test_store_column1";
    String storeDir = getTempDatabaseDir(storeName);;
    int valueSchemaId = 1;
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    Properties props = new Properties();
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, props);
    RocksDBServerConfig rocksDBServerConfig  = new RocksDBServerConfig(veniceServerProperties);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    TimestampMetadataRocksDBStoragePartition storagePartition = new TimestampMetadataRocksDBStoragePartition(partitionConfig, factory, DATA_BASE_DIR, null, rocksDbThrottler, rocksDBServerConfig);

    Map<String, Pair<String, String>> inputRecords = generateInputWithMetadata(100);
    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      // Use ByteBuffer value/metadata API here since it performs conversion from ByteBuffer to byte array
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(entry.getValue().getFirst().getBytes());
      int valuePosition = valueByteBuffer.position();

      byte[] replicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond().getBytes(), valueSchemaId);

      storagePartition.putWithTimestampMetadata(entry.getKey().getBytes(), valueByteBuffer, replicationMetadataWitValueSchemaIdBytes);
      Assert.assertEquals(valueByteBuffer.position(), valuePosition);
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = storagePartition.get(key);
      Assert.assertEquals(value, entry.getValue().getFirst().getBytes());
      ReplicationMetadataWithValueSchemaId replicationMetadataWithValueSchemaId =
          ReplicationMetadataWithValueSchemaId.getFromStorageEngineBytes(storagePartition.getTimestampMetadata(key));
      byte[] metadata = ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId.getReplicationMetadata());
      Assert.assertEquals(replicationMetadataWithValueSchemaId.getValueSchemaId(), valueSchemaId);
      Assert.assertEquals(metadata, entry.getValue().getSecond().getBytes());
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      int updatedValueSchemaId = 2;
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(entry.getKey().getBytes(), updatedReplicationMetadataWitValueSchemaIdBytes);

      byte[] value = storagePartition.get(entry.getKey().getBytes());
      Assert.assertNull(value);
      ReplicationMetadataWithValueSchemaId replicationMetadataWithValueSchemaId =
          ReplicationMetadataWithValueSchemaId.getFromStorageEngineBytes(storagePartition.getTimestampMetadata(entry.getKey().getBytes()));
      byte[] metadata = ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId.getReplicationMetadata());
      Assert.assertNotNull(metadata);
      Assert.assertEquals(replicationMetadataWithValueSchemaId.getValueSchemaId(), updatedValueSchemaId);
      Assert.assertEquals(metadata, updatedMetadataBytes);
    }



    // Records from Batch push may have no replication metadata
    Map<String, Pair<String, String>> inputRecordsBatch = generateInputWithMetadata(100, 200);
    for (Map.Entry<String, Pair<String, String>> entry : inputRecordsBatch.entrySet()) {
      // Use ByteBuffer value/metadata API here since it performs conversion from ByteBuffer to byte array
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(entry.getValue().getFirst().getBytes());
      int valuePosition = valueByteBuffer.position();
      ByteBuffer metadataByteBuffer = ByteBuffer.wrap(entry.getValue().getSecond().getBytes());
      int metadataPosition = metadataByteBuffer.position();
      storagePartition.put(entry.getKey().getBytes(), valueByteBuffer);
      Assert.assertEquals(valueByteBuffer.position(), valuePosition);
      Assert.assertEquals(metadataByteBuffer.position(), metadataPosition);
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecordsBatch.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = storagePartition.get(key);
      Assert.assertEquals(value, entry.getValue().getFirst().getBytes());
      Assert.assertNull(storagePartition.getTimestampMetadata(key));
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecordsBatch.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      int updatedValueSchemaId = 2;
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(entry.getKey().getBytes(), updatedReplicationMetadataWitValueSchemaIdBytes);


      byte[] value = storagePartition.get(entry.getKey().getBytes());
      Assert.assertNull(value);
      ReplicationMetadataWithValueSchemaId replicationMetadataWithValueSchemaId =
          ReplicationMetadataWithValueSchemaId.getFromStorageEngineBytes(storagePartition.getTimestampMetadata(entry.getKey().getBytes()));
      byte[] metadata = ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId.getReplicationMetadata());
      Assert.assertNotNull(metadata);
      Assert.assertEquals(replicationMetadataWithValueSchemaId.getValueSchemaId(), updatedValueSchemaId);
      Assert.assertEquals(metadata, updatedMetadataBytes);
    }

    storagePartition.drop();
    removeDir(storeDir);
  }

  private byte[] getReplicationMetadataWithValueSchemaId(byte[] replicationMetadata, int valueSchemaId) {
    ByteBuffer metadataByteBuffer = ByteBuffer.wrap(replicationMetadata);
    ByteBuffer replicationMetadataWitValueSchemaId = ByteUtils.prependIntHeaderToByteBuffer(metadataByteBuffer, valueSchemaId, false);
    replicationMetadataWitValueSchemaId.position(replicationMetadataWitValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWitValueSchemaId);
  }
}
