package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class ReplicationMetadataRocksDBStoragePartitionTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private final String storeName = Utils.getUniqueString("rocksdb_store_test");
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private final int versionNumber = 0;
  private final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();
  private static final String keyPrefix = "key_";
  private static final String valuePrefix = "value_";
  private static final String metadataPrefix = "metadata_";
  private static final RocksDBThrottler rocksDbThrottler = new RocksDBThrottler(3);
  private  MergeConflictResolver mergeConflictResolver;
  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;

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
    storeConfig = new VeniceStoreVersionConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
    String stringSchema = "\"string\"";
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(stringSchema, 1);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);

    ReplicationMetadataSchemaEntry
        rmdSchemaEntry = new ReplicationMetadataSchemaEntry(1, 1, aaSchema);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, stringSchema);
    ReplicationMetadataSchemaEntry rmdSchemaEnry = new ReplicationMetadataSchemaEntry(1, 1, aaSchema);
    doReturn(valueSchemaEntry).when(schemaRepository).getLatestValueSchema(anyString());
    doReturn(rmdSchemaEnry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    mergeConflictResolver = new MergeConflictResolver(schemaRepository, storeName, 1);
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
  public void testUseReplicationMetadataRocksDBStoragePartition() {
    // Verify that data partition is created as RMD-RocksDB Partition.
    Assert.assertTrue(testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof ReplicationMetadataRocksDBStoragePartition);
    // Verify that metadata partition is not create as RMD-RocksDB Partition
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof ReplicationMetadataRocksDBStoragePartition);
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
    ReplicationMetadataRocksDBStoragePartition
        storagePartition = new ReplicationMetadataRocksDBStoragePartition(partitionConfig, factory, DATA_BASE_DIR, null, rocksDbThrottler, rocksDBServerConfig);

    Map<String, Pair<String, String>> inputRecords = generateInputWithMetadata(100);
    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      // Use ByteBuffer value/metadata API here since it performs conversion from ByteBuffer to byte array
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(entry.getValue().getFirst().getBytes());
      int valuePosition = valueByteBuffer.position();

      byte[] replicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond().getBytes(), valueSchemaId);

      storagePartition.putWithReplicationMetadata(entry.getKey().getBytes(), valueByteBuffer, replicationMetadataWitValueSchemaIdBytes);
      Assert.assertEquals(valueByteBuffer.position(), valuePosition);
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = storagePartition.get(key);
      Assert.assertEquals(value, entry.getValue().getFirst().getBytes());
      byte[] metadata = storagePartition.getReplicationMetadata(key);
      ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(metadata);
      int replicationMetadataWithValueSchemaInt = replicationMetadataWithValueSchema.getInt();

      Assert.assertEquals(replicationMetadataWithValueSchemaInt, valueSchemaId);
      Assert.assertEquals(replicationMetadataWithValueSchema, ByteBuffer.wrap(entry.getValue().getSecond().getBytes()));
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      byte[] key = entry.getKey().getBytes();
      int updatedValueSchemaId = 2;
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(key, updatedReplicationMetadataWitValueSchemaIdBytes);

      byte[] value = storagePartition.get(key);
      Assert.assertNull(value);

      byte[] metadata = storagePartition.getReplicationMetadata(key);
      ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(metadata);
      int replicationMetadataWithValueSchemaInt = replicationMetadataWithValueSchema.getInt();

      Assert.assertNotNull(metadata);
      Assert.assertEquals(replicationMetadataWithValueSchema, ByteBuffer.wrap(updatedMetadataBytes));
      Assert.assertEquals(replicationMetadataWithValueSchemaInt, updatedValueSchemaId);
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
      Assert.assertNull(storagePartition.getReplicationMetadata(key));
    }

    for (Map.Entry<String, Pair<String, String>> entry : inputRecordsBatch.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      int updatedValueSchemaId = 2;
      byte[] key = entry.getKey().getBytes();
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes = getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(key, updatedReplicationMetadataWitValueSchemaIdBytes);


      byte[] value = storagePartition.get(key);
      Assert.assertNull(value);

      byte[] metadata = storagePartition.getReplicationMetadata(key);
      ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(metadata);
      int replicationMetadataWithValueSchemaInt = replicationMetadataWithValueSchema.getInt();

      Assert.assertNotNull(metadata);
      Assert.assertEquals(replicationMetadataWithValueSchemaInt, updatedValueSchemaId);
      Assert.assertEquals(replicationMetadataWithValueSchema, ByteBuffer.wrap(updatedMetadataBytes));
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
