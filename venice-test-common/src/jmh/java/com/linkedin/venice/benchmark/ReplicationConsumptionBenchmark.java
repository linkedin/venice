package com.linkedin.venice.benchmark;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.rocksdb.ReplicationMetadataRocksDBStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.davinci.store.rocksdb.RocksDBThrottler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;


@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReplicationConsumptionBenchmark {
  protected static final int KEY_COUNT = 1_000_000;
  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();
  private static final String keyPrefix = "key_";
  private static final String valuePrefix = "value_";
  private static final String metadataPrefix = "metadata_";
  private static final RocksDBThrottler rocksDbThrottler = new RocksDBThrottler(3);
  private String storeDir;
  private Map<String, Pair<String, String>> inputRecords;
  private Optional<CheckSum> runningChecksum;
  private ReplicationMetadataRocksDBStoragePartition storagePartition;
  private StoragePartitionConfig partitionConfig;
  private RocksDBServerConfig rocksDBServerConfig;
  private VeniceServerConfig serverConfig;
  private RocksDBStorageEngineFactory factory;
  private int syncPerRecords;
  private org.rocksdb.Options options;

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    runningChecksum = CheckSum.getInstance(CheckSumType.MD5);
    String storeName = Utils.getUniqueString("test_store");
    storeDir = getTempDatabaseDir(storeName);
    int partitionId = 0;
    partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    partitionConfig.setDeferredWrite(true);
    options = new org.rocksdb.Options();
    options.setCreateIfMissing(true);
    inputRecords = generateInputWithMetadata(0, KEY_COUNT);
    VeniceProperties veniceServerProperties = getServerProperties(PersistenceType.ROCKS_DB, new Properties());
    rocksDBServerConfig  = new RocksDBServerConfig(veniceServerProperties);
    serverConfig = new VeniceServerConfig(veniceServerProperties);
    factory = new RocksDBStorageEngineFactory(serverConfig);
    storagePartition = new ReplicationMetadataRocksDBStoragePartition(partitionConfig, factory, DATA_BASE_DIR, null, rocksDbThrottler, rocksDBServerConfig);
    syncPerRecords = 10000;

    // JMH benchmark relies on System.exit to finish one round of benchmark run, otherwise it will hang there.
    TestUtils.restoreSystemExit();
  }

  @TearDown
  public void cleanUp() {
    storagePartition.drop();
    options.close();
    removeDir(storeDir);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(ReplicationConsumptionBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }

  @Benchmark
  @OperationsPerInvocation(KEY_COUNT)
  public void benchmarkReplicationMetadataIngestion() {
    boolean sorted = true,  verifyChecksum = false;

    Optional<Supplier<byte[]>> checksumSupplier = Optional.empty();
    if (verifyChecksum) {
      checksumSupplier = Optional.of(() -> {
        byte[] checksum = runningChecksum.get().getCheckSum();
        runningChecksum.get().reset();
        return checksum;
      });
    }
    if (sorted) {
      storagePartition.beginBatchWrite(new HashMap<>(), checksumSupplier);
    }
    Map<String, String> checkpointingInfo = new HashMap<>();

    for (Map.Entry<String, Pair<String, String>> entry : inputRecords.entrySet()) {
      if (entry.getValue().getFirst() == null) {
        storagePartition.deleteWithReplicationMetadata(entry.getKey().getBytes(), entry.getValue().getSecond().getBytes());
      } else {
        storagePartition.putWithReplicationMetadata(entry.getKey().getBytes(), entry.getValue().getFirst().getBytes(),
            entry.getValue().getSecond().getBytes());
      }
    }

    storagePartition.endBatchWrite();

    // Re-open it in read/write mode
    storagePartition.close();
    partitionConfig.setDeferredWrite(false);
    partitionConfig.setWriteOnlyConfig(false);
    storagePartition = new ReplicationMetadataRocksDBStoragePartition(partitionConfig, factory, DATA_BASE_DIR, null, rocksDbThrottler, rocksDBServerConfig);
    // Test deletion
    String toBeDeletedKey = keyPrefix + 10;
   // Assert.assertNotNull(storagePartition.get(toBeDeletedKey.getBytes(), false));
    storagePartition.delete(toBeDeletedKey.getBytes());
    Assert.assertNull(storagePartition.get(toBeDeletedKey.getBytes(), false));
  }

  private Map<String, Pair<String, String>> generateInputWithMetadata(int startIndex, int endIndex) {
    Map<String, Pair<String, String>> records;
    BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
    records = new TreeMap<>((o1, o2) -> {
      ByteBuffer b1 = ByteBuffer.wrap(o1.getBytes());
      ByteBuffer b2 = ByteBuffer.wrap(o2.getBytes());
      return comparator.compare(b1, b2);
    });

    for (int i = startIndex; i < endIndex; ++i) {
      String value = i%100 == 0 ? null : valuePrefix + i;
      String metadata = metadataPrefix + i;
      records.put(keyPrefix + i, Pair.create(value, metadata));
    }
    return records;
  }

  private VeniceProperties getServerProperties(PersistenceType persistenceType, Properties properties) {
    File dataDirectory = Utils.getTempDataDirectory();
    return new PropertyBuilder()
        .put(CLUSTER_NAME, "test_offset_manager")
        .put(ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, "true")
        .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 1000)
        .put(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(PERSISTENCE_TYPE, persistenceType.toString())
        .put(KAFKA_BROKERS, "localhost")
        .put(KAFKA_BROKER_PORT, "9092")
        .put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092")
        .put(KAFKA_ZK_ADDRESS, "localhost:2181")
        .put(LISTENER_PORT , 7072)
        .put(ADMIN_PORT , 7073)
        .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(properties)
        .build();
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
}

