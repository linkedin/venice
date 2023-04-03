package com.linkedin.venice.benchmark;

import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
public class MixedIngestionBenchmark {
  private static final int NUM_RECORDS = 100_000;
  private VeniceClusterWrapper cluster;
  private VeniceServerWrapper serverWrapper;
  private int replicaFactor = 1;
  private int partitionSize = 1000;
  private long testTimeOutMS = 200000;
  private final String keyPrefix = "key_";
  private final String valuePrefix = "value_";

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    int numberOfController = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, 0, 0, replicaFactor, partitionSize, false, false);
    cluster.addVeniceServer(new Properties(getVeniceServerProperties()));
    // JMH benchmark relies on System.exit to finish one round of benchmark run, otherwise it will hang there.
    TestUtils.restoreSystemExit();
  }

  private Properties getVeniceServerProperties() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 100);
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 100);
    properties.put(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, true);

    return properties;
  }

  @TearDown
  public void cleanUp() throws InterruptedException {
    cluster.close();
  }

  @Benchmark
  public void ingestionBenchMark() throws Exception {
    String storeName = Utils.getUniqueString("new_store");
    long storageQuota = 1000000;
    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    VersionCreationResponse response = cluster.getNewVersion(storeName);

    String topicName = response.getKafkaTopic();
    Assert.assertEquals(response.getReplicas(), replicaFactor);

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      Map<String, String> records = generateInput(500000, true, 0);
      for (Map.Entry<String, String> entry: records.entrySet()) {
        veniceWriter.put(entry.getKey(), entry.getValue(), 1);
      }
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }
    cluster.createStore(50000);

    // Wait push completed.

    TestUtils.waitForNonDeterministicCompletion(
        testTimeOutMS,
        TimeUnit.MILLISECONDS,
        () -> cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(cluster.getClusterName(), topicName)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED));

  }

  private Map<String, String> generateInput(int recordCnt, boolean sorted, int startId) {
    final String keyPrefix = "key_";
    final String valuePrefix = "value_";
    Map<String, String> records;
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
    for (int i = startId; i < recordCnt + startId; ++i) {
      records.put(keyPrefix + i, valuePrefix + i);
    }
    return records;
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(MixedIngestionBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .build();
    new Runner(opt).run();
  }
}
