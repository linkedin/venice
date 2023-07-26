package com.linkedin.venice.benchmark;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngineFactory;
import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBThrottler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.RocksDB;


@Fork(value = 2, jvmArgs = { "-Xms4G", "-Xmx4G" })
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RocksDBLookupApiBenchmark {
  private static String getRandomStr(int length) {
    byte[] str = new byte[length];
    ThreadLocalRandom.current().nextBytes(str);
    return new String(str);
  }

  private static final String DATA_BASE_DIR = Utils.getUniqueTempPath();

  private static final String KEY_PREFIX = getRandomStr(10);
  private static final String VALUE_PREFIX = getRandomStr(100);
  private static final int ROW_CNT = 5_000_000;

  private RocksDBStoragePartition storagePartition;
  private String storeDir;

  @Param({ "1", "2", "5", "10", "50" })
  private static int BATCH_SIZE;

  private String getTempDatabaseDir(String storeName) {
    File storeDir = new File(DATA_BASE_DIR, storeName).getAbsoluteFile();
    if (!storeDir.mkdirs()) {
      throw new VeniceException("Failed to mkdirs for path: " + storeDir.getPath());
    }
    storeDir.deleteOnExit();
    return storeDir.getPath();
  }

  public static VeniceProperties getServerProperties(PersistenceType persistenceType, Properties properties) {
    return new PropertyBuilder().put(CLUSTER_NAME, "test_offset_manager")
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(PERSISTENCE_TYPE, persistenceType.toString())
        .put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092")
        .put(LISTENER_PORT, 7072)
        .put(ADMIN_PORT, 7073)
        .put(DATA_BASE_PATH, DATA_BASE_DIR)
        .put(properties)
        .build();
  }

  @Setup
  public void setUp() throws Exception {
    RocksDB.loadLibrary();
    String storeName = Utils.getUniqueString("test_store");
    storeDir = getTempDatabaseDir(storeName);
    Properties properties = new Properties();
    properties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString());
    properties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    properties.put(ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, 2);
    properties.put(ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER, 4);
    properties.put(ROCKSDB_LEVEL0_STOPS_WRITES_TRIGGER, 6);
    VeniceProperties veniceServerProperties = getServerProperties(PersistenceType.ROCKS_DB, properties);
    storagePartition = new RocksDBStoragePartition(
        new StoragePartitionConfig(storeName, 0),
        new RocksDBStorageEngineFactory(new VeniceServerConfig(veniceServerProperties)),
        DATA_BASE_DIR,
        null,
        new RocksDBThrottler(3),
        new RocksDBServerConfig(veniceServerProperties));

    // Populate the database
    for (int i = 0; i < ROW_CNT; ++i) {
      byte[] key = (KEY_PREFIX + i).getBytes();
      byte[] value = (VALUE_PREFIX + i).getBytes();
      storagePartition.put(key, value);
    }
    System.out.println("Finished populating the database, path: " + storagePartition.getFullPathForTempSSTFileDir());
  }

  @TearDown
  public void tearDown() {
    storagePartition.drop();
    File file = new File(storeDir);
    if (file.exists() && !file.delete()) {
      throw new VeniceException("Failed to remove path: " + storeDir);
    }
  }

  @Benchmark
  public void measureSingleGetAPI(org.openjdk.jmh.infra.Blackhole bh) {
    if (BATCH_SIZE != 1) {
      // Only execute this function once.
      return;
    }
    for (int cur = 0; cur < ROW_CNT; ++cur) {
      bh.consume(storagePartition.get((KEY_PREFIX + cur).getBytes()));
    }
  }

  @Benchmark
  public void measureMultiGetAPI(org.openjdk.jmh.infra.Blackhole bh) {
    List<byte[]> keys = new ArrayList<>(BATCH_SIZE);
    // populate with dummy elements
    for (int i = 0; i < BATCH_SIZE; ++i) {
      keys.add(null);
    }
    for (int cur = 0; cur < ROW_CNT; cur += BATCH_SIZE) {
      for (int b = 0; b < BATCH_SIZE; ++b) {
        keys.set(b, (KEY_PREFIX + cur + b).getBytes());
      }
      bh.consume(storagePartition.multiGet(keys));
    }
  }

  @Benchmark
  public void measureMultiGetAPIWithByteBuffer(org.openjdk.jmh.infra.Blackhole bh) {
    List<ByteBuffer> keys = new ArrayList<>(BATCH_SIZE);
    List<ByteBuffer> values = new ArrayList<>(BATCH_SIZE);
    // populate with dummy elements
    for (int i = 0; i < BATCH_SIZE; ++i) {
      keys.add(ByteBuffer.allocateDirect(50)); // make sure it is large enough
      values.add(ByteBuffer.allocateDirect(200));
    }

    for (int cur = 0; cur < ROW_CNT; cur += BATCH_SIZE) {
      for (int b = 0; b < BATCH_SIZE; ++b) {
        ByteBuffer keyBuffer = keys.get(b);
        keyBuffer.clear();
        keyBuffer.put((KEY_PREFIX + cur + b).getBytes());
        keyBuffer.flip();
        values.get(b).clear();
      }
      bh.consume(storagePartition.multiGet(keys, values));
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(RocksDBLookupApiBenchmark.class.getSimpleName()).build();
    new Runner(opt).run();
  }
}
