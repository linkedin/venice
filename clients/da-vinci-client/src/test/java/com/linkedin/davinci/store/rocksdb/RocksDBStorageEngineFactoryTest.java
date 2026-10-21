package com.linkedin.davinci.store.rocksdb;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.Priority;
import org.rocksdb.RocksDB;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBStorageEngineFactoryTest {
  @Test
  public void testNativeMetricsPreserveListenersAndFactoryOwnsTheirLifetime() throws Exception {
    VeniceProperties properties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    try (VeniceMetricsRepository repository =
        new VeniceMetricsRepository(new VeniceMetricsConfig.Builder().setEmitOtelMetrics(false).build())) {
      RocksDBMemoryStats stats = new RocksDBMemoryStats(repository, "rocksdb", false, "test-cluster");
      RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(
          new VeniceServerConfig(properties),
          stats,
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
          AvroProtocolDefinition.PARTITION_STATE.getSerializer());
      AtomicInteger existingCompletions = new AtomicInteger();
      AbstractEventListener backgroundListener;
      try (AbstractEventListener existing =
          new AbstractEventListener(AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED) {
            @Override
            public void onFlushCompleted(RocksDB db, FlushJobInfo info) {
              existingCompletions.incrementAndGet();
            }
          };
          Options options = new Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
          FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        options.setEnv(factory.getEnv());
        options.setListeners(Collections.singletonList(existing));
        factory.addBackgroundEventListener(options);
        Assert.assertEquals(options.listeners().size(), 2);
        Assert.assertSame(options.listeners().get(0), existing);
        backgroundListener = options.listeners().get(1);
        try {
          File databaseDirectory = new File(factory.getRocksDBPath("native-metrics", 0));
          Assert.assertTrue(databaseDirectory.getParentFile().mkdirs());
          try (RocksDB db = RocksDB.open(options, factory.getRocksDBPath("native-metrics", 0))) {
            db.put(new byte[] { 1 }, new byte[] { 2 });
            db.flush(flushOptions);
            db.put(new byte[] { 1 }, new byte[] { 3 });
            db.flush(flushOptions);
            db.compactRange();
            Assert.assertEquals(existingCompletions.get(), 2);
            Assert.assertEquals(repository.getMetric(".rocksdb--rocksdb.flush.completed_count.Count").value(), 2.0);
            Assert.assertTrue(repository.getMetric(".rocksdb--rocksdb.flush.time.Avg").value() >= 0);
            Assert.assertTrue(repository.getMetric(".rocksdb--rocksdb.flush.completed_raw_bytes.Total").value() > 0);
            Assert.assertTrue(repository.getMetric(".rocksdb--rocksdb.compaction.completed_count.Count").value() > 0);
            Assert.assertTrue(repository.getMetric(".rocksdb--rocksdb.compaction.time.Avg").value() >= 0);
          }
          Assert.assertTrue(backgroundListener.isOwningHandle(), "Listener must outlive DB close");
          VeniceStoreVersionConfig managedConfig =
              new VeniceStoreVersionConfig("native_metrics_v1", properties, PersistenceType.ROCKS_DB);
          StorageEngine engine = factory.getStorageEngine(managedConfig);
          engine.addStoragePartitionIfAbsent(0);
          engine.put(0, new byte[] { 1 }, new byte[] { 2 });
          engine.sync(0);
          Assert.assertTrue(repository.getMetric(".rocksdb--rocksdb.flush.completed_count.Count").value() > 2);
        } finally {
          factory.close();
        }
        Assert.assertFalse(backgroundListener.isOwningHandle());
        Assert.assertTrue(existing.isOwningHandle(), "Factory must not close listeners it does not own");
        Assert.assertTrue(factory.getEnv().getBackgroundThreads(Priority.HIGH) > 0);
      }
    }
  }

  @Test
  public void testRocksDBCreation() {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = Version.composeKafkaTopic(Utils.getUniqueString("test_store"), 1);
    VeniceStoreVersionConfig testStoreConfig =
        new VeniceStoreVersionConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    StorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
    Assert.assertTrue(
        storeEngine instanceof RocksDBStorageEngine,
        "Database generated by" + " 'RocksDBStorageEngineFactory' must be 'RocksDBStorageEngine' instance");
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
