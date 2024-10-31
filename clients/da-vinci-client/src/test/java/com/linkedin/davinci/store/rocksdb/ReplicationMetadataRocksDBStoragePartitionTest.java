package com.linkedin.davinci.store.rocksdb;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
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
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ReplicationMetadataRocksDBStoragePartitionTest extends AbstractStorageEngineTest {
  private static final int PARTITION_ID = 0;

  private static final String storeName = Version.composeKafkaTopic(Utils.getUniqueString("rocksdb_store_test"), 1);
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final int versionNumber = 0;
  private static final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();
  private static final String KEY_PREFIX = "key_";
  private static final String VALUE_PREFIX = "value_";
  private static final String METADATA_PREFIX = "metadata_";
  private static final RocksDBThrottler ROCKSDB_THROTTLER = new RocksDBThrottler(3);
  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;

  protected Map<String, Pair<String, String>> generateInputWithMetadata(int recordCnt) {
    return generateInputWithMetadata(0, recordCnt, false, false);
  }

  private Map<String, Pair<String, String>> generateInputWithMetadata(
      int startIndex,
      int endIndex,
      boolean sorted,
      boolean createTombStone) {
    Map<String, Pair<String, String>> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        ByteBuffer b1 = ByteBuffer.wrap(o1.getBytes());
        ByteBuffer b2 = ByteBuffer.wrap(o2.getBytes());
        return comparator.compare(b1, b2);
      });
    } else {
      records = new HashMap<>();
    }
    for (int i = startIndex; i < endIndex; ++i) {
      String value = createTombStone && i % 100 == 0 ? null : VALUE_PREFIX + i;
      String metadata = METADATA_PREFIX + i;
      records.put(KEY_PREFIX + i, Pair.create(value, metadata));
    }
    return records;
  }

  protected String getTempDatabaseDir(String storeName) {
    File storeDir = new File(DATA_BASE_DIR, storeName).getAbsoluteFile();
    if (!storeDir.mkdirs()) {
      throw new VeniceException("Failed to mkdirs for path: " + storeDir.getPath());
    }
    storeDir.deleteOnExit();
    return storeDir.getPath();
  }

  protected void removeDir(String path) {
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
    when(mockStore.getVersion(versionNumber)).thenReturn(mockVersion);
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
    testStoreEngine = storageService.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null);
    createStoreForTest();
    String stringSchema = "\"string\"";
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(stringSchema, 1);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);

    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, 1, aaSchema);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, stringSchema);
    RmdSchemaEntry rmdSchemaEnry = new RmdSchemaEntry(1, 1, aaSchema);
    doReturn(valueSchemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(anyString());
    doReturn(rmdSchemaEnry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());
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

  @Test
  public void testUseReplicationMetadataRocksDBStoragePartition() {
    // Verify that data partition is created as RMD-RocksDB Partition.
    Assert.assertTrue(
        testStoreEngine.getPartitionOrThrow(PARTITION_ID) instanceof ReplicationMetadataRocksDBStoragePartition);
    // Verify that metadata partition is not create as RMD-RocksDB Partition
    Assert.assertFalse(testStoreEngine.getMetadataPartition() instanceof ReplicationMetadataRocksDBStoragePartition);
  }

  @Test
  public void testMetadataColumnFamily() {
    String storeName = Version.composeKafkaTopic("test_store_column1", 1);
    String storeDir = getTempDatabaseDir(storeName);
    ;
    int valueSchemaId = 1;
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    Properties props = new Properties();
    VeniceProperties veniceServerProperties =
        AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, props);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    ReplicationMetadataRocksDBStoragePartition storagePartition = new ReplicationMetadataRocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    Map<String, Pair<String, String>> inputRecords = generateInputWithMetadata(100);
    for (Map.Entry<String, Pair<String, String>> entry: inputRecords.entrySet()) {
      // Use ByteBuffer value/metadata API here since it performs conversion from ByteBuffer to byte array
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(entry.getValue().getFirst().getBytes());
      int valuePosition = valueByteBuffer.position();

      byte[] replicationMetadataWitValueSchemaIdBytes =
          getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond().getBytes(), valueSchemaId);

      storagePartition.putWithReplicationMetadata(
          entry.getKey().getBytes(),
          valueByteBuffer,
          replicationMetadataWitValueSchemaIdBytes);
      Assert.assertEquals(valueByteBuffer.position(), valuePosition);
    }

    for (Map.Entry<String, Pair<String, String>> entry: inputRecords.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = storagePartition.get(key);
      Assert.assertEquals(value, entry.getValue().getFirst().getBytes());
      byte[] metadata = storagePartition.getReplicationMetadata(ByteBuffer.wrap(key));
      ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(metadata);
      int replicationMetadataWithValueSchemaInt = replicationMetadataWithValueSchema.getInt();

      Assert.assertEquals(replicationMetadataWithValueSchemaInt, valueSchemaId);
      Assert.assertEquals(replicationMetadataWithValueSchema, ByteBuffer.wrap(entry.getValue().getSecond().getBytes()));
    }

    for (Map.Entry<String, Pair<String, String>> entry: inputRecords.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      byte[] key = entry.getKey().getBytes();
      int updatedValueSchemaId = 2;
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes =
          getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(key, updatedReplicationMetadataWitValueSchemaIdBytes);

      byte[] value = storagePartition.get(key);
      Assert.assertNull(value);

      byte[] metadata = storagePartition.getReplicationMetadata(ByteBuffer.wrap(key));
      ByteBuffer replicationMetadataWithValueSchema = ByteBuffer.wrap(metadata);
      int replicationMetadataWithValueSchemaInt = replicationMetadataWithValueSchema.getInt();

      Assert.assertNotNull(metadata);
      Assert.assertEquals(replicationMetadataWithValueSchema, ByteBuffer.wrap(updatedMetadataBytes));
      Assert.assertEquals(replicationMetadataWithValueSchemaInt, updatedValueSchemaId);
    }

    // Records from Batch push may have no replication metadata
    Map<String, Pair<String, String>> inputRecordsBatch = generateInputWithMetadata(100, 200, false, false);
    for (Map.Entry<String, Pair<String, String>> entry: inputRecordsBatch.entrySet()) {
      // Use ByteBuffer value/metadata API here since it performs conversion from ByteBuffer to byte array
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(entry.getValue().getFirst().getBytes());
      int valuePosition = valueByteBuffer.position();
      ByteBuffer metadataByteBuffer = ByteBuffer.wrap(entry.getValue().getSecond().getBytes());
      int metadataPosition = metadataByteBuffer.position();
      storagePartition.put(entry.getKey().getBytes(), valueByteBuffer);
      Assert.assertEquals(valueByteBuffer.position(), valuePosition);
      Assert.assertEquals(metadataByteBuffer.position(), metadataPosition);
    }

    for (Map.Entry<String, Pair<String, String>> entry: inputRecordsBatch.entrySet()) {
      byte[] key = entry.getKey().getBytes();
      byte[] value = storagePartition.get(key);
      Assert.assertEquals(value, entry.getValue().getFirst().getBytes());
      Assert.assertNull(storagePartition.getReplicationMetadata(ByteBuffer.wrap(key)));
    }

    for (Map.Entry<String, Pair<String, String>> entry: inputRecordsBatch.entrySet()) {
      byte[] updatedMetadataBytes = "updated_metadata".getBytes();
      int updatedValueSchemaId = 2;
      byte[] key = entry.getKey().getBytes();
      byte[] updatedReplicationMetadataWitValueSchemaIdBytes =
          getReplicationMetadataWithValueSchemaId(updatedMetadataBytes, updatedValueSchemaId);

      storagePartition.deleteWithReplicationMetadata(key, updatedReplicationMetadataWitValueSchemaIdBytes);

      byte[] value = storagePartition.get(key);
      Assert.assertNull(value);

      byte[] metadata = storagePartition.getReplicationMetadata(ByteBuffer.wrap(key));
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
    ByteBuffer replicationMetadataWitValueSchemaId =
        ByteUtils.prependIntHeaderToByteBuffer(metadataByteBuffer, valueSchemaId, false);
    replicationMetadataWitValueSchemaId
        .position(replicationMetadataWitValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWitValueSchemaId);
  }

  @Test(dataProvider = "testIngestionDataProvider")
  public void testReplicationMetadataIngestion(
      boolean sorted,
      boolean interrupted,
      boolean reopenDatabaseDuringInterruption,
      boolean verifyChecksum) {
    CheckSum runningChecksum = CheckSum.getInstance(CheckSumType.MD5);
    String storeName = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    String storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(sorted);
    Options options = new Options();
    options.setCreateIfMissing(true);
    Map<String, Pair<String, String>> inputRecords = generateInputWithMetadata(0, 1000, sorted, true);
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    RocksDBServerConfig rocksDBServerConfig = new RocksDBServerConfig(veniceServerProperties);

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    VeniceStoreVersionConfig storeConfig = new VeniceStoreVersionConfig(storeName, veniceServerProperties);
    ReplicationMetadataRocksDBStoragePartition storagePartition = new ReplicationMetadataRocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);

    final int syncPerRecords = 100;
    final int interruptedRecord = 345;

    Optional<Supplier<byte[]>> checksumSupplier = Optional.empty();
    if (verifyChecksum) {
      checksumSupplier = Optional.of(() -> {
        byte[] checksum = runningChecksum.getCheckSum();
        runningChecksum.reset();
        return checksum;
      });
    }
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);
    }
    int currentRecordNum = 0;
    int currentFileNo = 0;
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, Pair<String, String>> entry: inputRecords.entrySet()) {
      if (entry.getValue().getFirst() == null) {
        storagePartition
            .deleteWithReplicationMetadata(entry.getKey().getBytes(), entry.getValue().getSecond().getBytes());
      } else {
        storagePartition.putWithReplicationMetadata(
            entry.getKey().getBytes(),
            entry.getValue().getFirst().getBytes(),
            entry.getValue().getSecond().getBytes());
      }
      if (verifyChecksum) {
        if (entry.getValue().getFirst() != null) {
          runningChecksum.update(entry.getKey().getBytes());
          runningChecksum.update(entry.getValue().getFirst().getBytes());
        }
      }
      if (++currentRecordNum % syncPerRecords == 0) {
        checkpointingInfo = storagePartition.sync();
        if (sorted) {
          Assert.assertEquals(
              checkpointingInfo.get(RocksDBSstFileWriter.ROCKSDB_LAST_FINISHED_SST_FILE_NO),
              Integer.toString(currentFileNo++));
        } else {
          Assert.assertTrue(
              checkpointingInfo.isEmpty(),
              "For non-deferred-write database, sync() should return empty map");
        }
      }
      if (interrupted) {
        if (currentRecordNum == interruptedRecord) {
          if (reopenDatabaseDuringInterruption) {
            storagePartition.close();
            storagePartition = new ReplicationMetadataRocksDBStoragePartition(
                partitionConfig,
                factory,
                DATA_BASE_DIR,
                null,
                ROCKSDB_THROTTLER,
                rocksDBServerConfig,
                storeConfig);
            Options storeOptions = storagePartition.getOptions();
            Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 100);
          }
          if (sorted) {
            storagePartition.beginBatchWrite(checkpointingInfo, checksumSupplier);
          }

          // Pass last checkpointed info.
          // Need to re-consume from the offset when last checkpoint happens
          // inclusive [replayStart, replayEnd]
          int replayStart = (interruptedRecord / syncPerRecords) * syncPerRecords + 1;
          int replayCnt = 0;
          runningChecksum.reset();
          for (Map.Entry<String, Pair<String, String>> innerEntry: inputRecords.entrySet()) {
            ++replayCnt;
            if (replayCnt >= replayStart && replayCnt <= interruptedRecord) {
              if (innerEntry.getValue().getFirst() == null) {
                storagePartition.deleteWithReplicationMetadata(
                    innerEntry.getKey().getBytes(),
                    innerEntry.getValue().getSecond().getBytes());
              } else {
                storagePartition.putWithReplicationMetadata(
                    innerEntry.getKey().getBytes(),
                    innerEntry.getValue().getFirst().getBytes(),
                    innerEntry.getValue().getSecond().getBytes());
              }
              if (verifyChecksum) {
                if (innerEntry.getValue().getFirst() != null) {
                  runningChecksum.update(innerEntry.getKey().getBytes());
                  runningChecksum.update(innerEntry.getValue().getFirst().getBytes());
                }
              }
            }
            if (replayCnt > interruptedRecord) {
              break;
            }
          }
        }
      }
    }

    if (sorted) {
      Assert.assertFalse(storagePartition.validateBatchIngestion());
      storagePartition.endBatchWrite();
      Assert.assertTrue(storagePartition.validateBatchIngestion());
    }

    // Verify all the key/value pairs
    for (Map.Entry<String, Pair<String, String>> entry: inputRecords.entrySet()) {
      byte[] bytes = entry.getValue().getFirst() == null ? null : entry.getValue().getFirst().getBytes();
      Assert.assertEquals(storagePartition.get(entry.getKey().getBytes()), bytes);
      if (sorted) {
        Assert.assertEquals(
            storagePartition.getReplicationMetadata(ByteBuffer.wrap(entry.getKey().getBytes())),
            entry.getValue().getSecond().getBytes());
      }
    }

    // Verify current ingestion mode is in deferred-write mode
    Assert.assertTrue(storagePartition.verifyConfig(partitionConfig));

    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    partitionConfig.setWriteOnlyConfig(false);
    storagePartition = new ReplicationMetadataRocksDBStoragePartition(
        partitionConfig,
        factory,
        DATA_BASE_DIR,
        null,
        ROCKSDB_THROTTLER,
        rocksDBServerConfig,
        storeConfig);
    // Test deletion
    String toBeDeletedKey = KEY_PREFIX + 10;
    Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes()));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes()));

    Options storeOptions = storagePartition.getOptions();
    Assert.assertEquals(storeOptions.level0FileNumCompactionTrigger(), 40);
    storagePartition.drop();
    options.close();
    removeDir(storeDir);
  }

  @DataProvider(name = "testIngestionDataProvider")
  protected Object[][] testIngestionDataProvider() {
    return new Object[][] { { true, false, false, true }, // Sorted input without interruption, with verifyChecksum
        { true, false, false, false }, // Sorted input without interruption, without verifyChecksum
        { true, true, true, false }, // Sorted input with interruption, without verifyChecksum
        { true, true, false, false }, // Sorted input with storage node re-boot, without verifyChecksum
        { true, true, true, true }, // Sorted input with interruption, with verifyChecksum
        { true, true, false, true }, // Sorted input with storage node re-boot, with verifyChecksum
        { false, false, false, false }, // Unsorted input without interruption, without verifyChecksum
        { false, true, false, false }, // Unsorted input with interruption, without verifyChecksum
        { false, true, true, false } // Unsorted input with storage node re-boot, without verifyChecksum
    };
  }
}
