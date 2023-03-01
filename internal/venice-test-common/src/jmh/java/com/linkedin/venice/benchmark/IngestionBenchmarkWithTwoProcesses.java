package com.linkedin.venice.benchmark;

import static com.linkedin.davinci.kafka.consumer.KafkaConsumerService.ConsumerAssignmentStrategy;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.ConfigKeys.UNSORTED_INPUT_DRAINER_SIZE;
import static com.linkedin.venice.integration.utils.ServiceFactory.getGenericAvroDaVinciClientWithRetries;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.Assert;


/**
 * Benchmark ingestion GC performance with JMH.
 *
 * We call it IngestionBenchmarkWithTwoProcesses since the main process only starts up a Da Vinci client
 * to do ingestion, while the testing cluster (including server, controller, kafka broker etc.) is spawned
 * in another process to maximize testing environment isolation.
 */
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(VeniceClusterWrapper.NUM_RECORDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class IngestionBenchmarkWithTwoProcesses {
  @Param({ "TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY", "PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY" })
  private static String sharedConsumerAssignmentStrategy;

  @Param({ "1", "2", "4" })
  private static int drainerSize;

  private String storeName;

  /**
   * Cluster info file works as an IPC to get needed parameters value from a remote process,
   * which spawns the testing Venice cluster.
   */
  private String clusterInfoFilePath;
  private String forkedProcessException;
  private String zkAddress;

  @Setup
  public void setUp() throws Exception {
    clusterInfoFilePath = File.createTempFile("temp-cluster-info", null).getAbsolutePath();
    ServiceFactory.startVeniceClusterInAnotherProcess(clusterInfoFilePath);
    // We need ot make sure Venice cluster in forked process is up and store has been created before we run our
    // benchmark.
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        true,
        () -> assertTrue(parseClusterInfoFile(), "The cluster info file should be parsable."));

    if (forkedProcessException != null) {
      Assert.fail("Got an exception in the forked process: " + forkedProcessException);
    }

    TestUtils.restoreSystemExit();
  }

  @TearDown
  public void cleanUp() {
    ServiceFactory.stopVeniceClusterInAnotherProcess();
    try {
      Files.delete(Paths.get(clusterInfoFilePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public void ingestionBenchmarkTest(Blackhole blackhole) throws IOException {
    File dataBasePath = Utils.getTempDataDirectory();
    try {
      FileUtils.deleteDirectory(dataBasePath);
      Map<String, Object> backendConfig = new HashMap<>();
      backendConfig.put(ConfigKeys.DATA_BASE_PATH, dataBasePath);
      backendConfig.put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
      backendConfig.put(RocksDBServerConfig.ROCKSDB_PUT_REUSE_BYTE_BUFFER, true);
      ConsumerAssignmentStrategy strategy = ConsumerAssignmentStrategy.valueOf(sharedConsumerAssignmentStrategy);
      backendConfig.put(SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY, strategy);
      backendConfig.put(SORTED_INPUT_DRAINER_SIZE, drainerSize);
      backendConfig.put(UNSORTED_INPUT_DRAINER_SIZE, drainerSize);

      DaVinciClient<String, String> client = getGenericAvroDaVinciClientWithRetries(
          storeName,
          zkAddress,
          new DaVinciConfig().setStorageClass(StorageClass.DISK),
          backendConfig);
      // Ingest data to local folder.
      client.subscribeAll().get(120, TimeUnit.SECONDS);
      client.close();
    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
      throw new VeniceException(e);
    } finally {
      FileUtils.deleteDirectory(dataBasePath);
    }
    blackhole.consume(dataBasePath);
  }

  private boolean parseClusterInfoFile() {
    try {
      VeniceProperties properties = Utils.parseProperties(clusterInfoFilePath);
      if (properties.containsKey(VeniceClusterWrapper.FORKED_PROCESS_EXCEPTION)) {
        forkedProcessException = properties.getString(VeniceClusterWrapper.FORKED_PROCESS_EXCEPTION);
      } else {
        storeName = properties.getString(VeniceClusterWrapper.FORKED_PROCESS_STORE_NAME);
        zkAddress = properties.getString(VeniceClusterWrapper.FORKED_PROCESS_ZK_ADDRESS);
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(IngestionBenchmarkWithTwoProcesses.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
    new Runner(opt).run();
  }
}
