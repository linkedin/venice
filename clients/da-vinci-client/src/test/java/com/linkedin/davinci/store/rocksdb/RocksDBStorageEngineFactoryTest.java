package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.AbstractStorageEngine.METADATA_PARTITION_ID;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ToLongFunction;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBStorageEngineFactoryTest {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRocksDBCreationAndMetrics(boolean replicationMetadataEnabled) throws IOException {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);

    MetricsRepository repository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Set<String> registrations = new HashSet<>();
    try {
      RocksDBMemoryStats stats = new RocksDBMemoryStats(repository, "partition", false, "test") {
        @Override
        public void registerPartition(String name, RocksDBStoragePartition partition) {
          Assert.fail("Automatic metrics registration must not publish the partition object");
        }

        @Override
        public void registerPartitionMetrics(String name, ToLongFunction<String> readStat) {
          Assert.assertTrue(registrations.add(name), "Duplicate metrics registration");
          super.registerPartitionMetrics(name, readStat);
        }
      };
      RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(
          serverConfig,
          stats,
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
          AvroProtocolDefinition.PARTITION_STATE.getSerializer());
      try {
        String property = "rocksdb.num-live-versions";
        Metric gauge = repository.getMetric(".partition--" + property + ".Gauge");
        RocksDBStorageEngine engine =
            (RocksDBStorageEngine) factory.getStorageEngine(testStoreConfig, replicationMetadataEnabled);
        double metadataVersions = gauge.value();
        engine.addStoragePartitionIfAbsent(0);
        engine.addStoragePartition(new StoragePartitionConfig(testStore, 1, true));
        RocksDBStoragePartition partition = engine.getPartitionOrThrow(0);
        Assert
            .assertEquals(partition instanceof ReplicationMetadataRocksDBStoragePartition, replicationMetadataEnabled);
        Assert.assertFalse(engine.getMetadataPartition() instanceof ReplicationMetadataRocksDBStoragePartition);
        Assert.assertEquals(
            registrations,
            Set.of(
                RocksDBUtils.getPartitionDbName(testStore, METADATA_PARTITION_ID),
                RocksDBUtils.getPartitionDbName(testStore, 0)));
        Assert.assertThrows(VeniceException.class, () -> engine.getPartitionOrThrow(1).getRocksDBStatValue(property));
        long versions = partition.getRocksDBStatValue(property);
        Assert.assertTrue(versions > 0 && metadataVersions > 0);
        Assert.assertEquals(gauge.value(), metadataVersions + versions);
        partition.reopen();
        Assert.assertEquals(gauge.value(), metadataVersions + partition.getRocksDBStatValue(property));
        engine.closePartition(0);
        Assert.assertEquals(gauge.value(), metadataVersions);
      } finally {
        factory.close();
        FileUtils.deleteDirectory(new File(serverConfig.getRocksDBPath()));
      }
    } finally {
      repository.close();
    }
  }

  @Test
  public void testGetPersistedStoreNames() {
    // Create two databases
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore1 = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig1 =
        new VeniceStoreVersionConfig(testStore1, veniceServerProperties, PersistenceType.ROCKS_DB);
    factory.getStorageEngine(testStoreConfig1);
    final String testStore2 = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig2 =
        new VeniceStoreVersionConfig(testStore2, veniceServerProperties, PersistenceType.ROCKS_DB);
    factory.getStorageEngine(testStoreConfig2);
    factory.close();
    // Retrieve all the persisted stores from disk
    Set<String> storeNameSet = factory.getPersistedStoreNames();
    Assert.assertEquals(storeNameSet.size(), 2);
    Assert.assertTrue(storeNameSet.contains(testStore1));
    Assert.assertTrue(storeNameSet.contains(testStore2));
  }

  @Test
  public void testRemoveStorageEngine() {
    // Create one databases
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storageEngine = factory.getStorageEngine(testStoreConfig);
    // drop the database
    factory.removeStorageEngine(storageEngine);
    factory.close();
    // Retrieve all the persisted stores from disk
    Set<String> storeNameSet = factory.getPersistedStoreNames();
    Assert.assertEquals(storeNameSet.size(), 0);
  }

  @Test
  public void testRemoveStorageEnginePartition() {
    // Create one databases
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storageEngine = factory.getStorageEngine(testStoreConfig);
    File file = new File(factory.getRocksDBPath(testStore, 1));
    file.mkdirs();
    Assert.assertTrue(file.exists());
    // drop the database
    factory.removeStorageEngine(storageEngine);
    factory.removeStorageEnginePartition(testStore, 1);
    factory.close();
    Assert.assertFalse(file.exists());
  }

  @Test
  public void testAddNewPartitionAfterRemoving() {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store_"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
    storeEngine.addStoragePartitionIfAbsent(1);
    storeEngine.dropPartition(1);
    factory.removeStorageEngine(storeEngine);
    // This could happen when re-balance happens or the pre-created partitions get moved/removed.
    storeEngine = factory.getStorageEngine(testStoreConfig);
    storeEngine.addStoragePartitionIfAbsent(1);

    factory.removeStorageEngine(storeEngine);
  }

  @Test
  public void testSstFileManagerWithDefaultConfig() {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    Assert.assertNotNull(factory.getSstFileManager(), "SstFileManager should be created");

    factory.close();
  }

  @Test
  public void testSstFileManagerWithDeletionRateLimiting() {
    long deletionRate = 64L * 1024 * 1024; // 64 MB/s

    VeniceProperties veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_DELETE_RATE_BYTES_PER_SECOND, String.valueOf(deletionRate))
        .build();

    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    Assert.assertNotNull(factory.getSstFileManager(), "SstFileManager should be created");

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
    Assert.assertNotNull(storeEngine, "Storage engine should be created successfully");
    factory.removeStorageEngine(storeEngine);
    factory.close();
  }

  @Test
  public void testSstFileManagerWithMaxTrashDBRatio() {
    double maxTrashRatio = 0.5;

    VeniceProperties veniceServerProperties = new PropertyBuilder()
        .put(AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB).toProperties())
        .put(RocksDBServerConfig.ROCKSDB_SST_FILE_MANAGER_MAX_TRASH_DB_RATIO, String.valueOf(maxTrashRatio))
        .build();
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);
    Assert.assertNotNull(factory.getSstFileManager(), "SstFileManager should be created");

    factory.close();
  }
}
