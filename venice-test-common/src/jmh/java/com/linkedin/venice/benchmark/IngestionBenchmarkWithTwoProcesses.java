package com.linkedin.venice.benchmark;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
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
import org.testng.Assert;

import static com.linkedin.venice.integration.utils.ServiceFactory.*;

/**
 * Benchmark ingestion GC performance with JMH.
 *
 * We call it IngestionBenchmarkWithTwoProcesses since the main process only starts up a Da Vinci client
 * to do ingestion, while the testing cluster (including server, controller, kafka broker etc.) is spawned
 * in another process to maximize testing environment isolation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 10)
public class IngestionBenchmarkWithTwoProcesses {
  /**
   * Cluster info file works as an IPC to get needed parameters value from a remote process,
   * which spawns the testing Venice cluster.
   */
  private String clusterInfoFilePath;
  private String storeName;
  private String zkAddress;

  @Setup
  public void setup() throws Exception {
    clusterInfoFilePath = File.createTempFile("temp-cluster-info", null).getAbsolutePath();
    ServiceFactory.startVeniceClusterInAnotherProcess(clusterInfoFilePath);

    // JMH benchmark relies on System.exit to finish one round of benchmark run, otherwise it will hang there.
    TestUtils.restoreSystemExit();
  }

  @TearDown
  public void teardown() throws InterruptedException {
    ServiceFactory.stopVeniceClusterInAnotherProcess();
    try {
      Files.delete(Paths.get(clusterInfoFilePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public void ingestionBenchmarkTest() {
    parseClusterInfoFile();
    File dataBasePath = TestUtils.getTempDataDirectory();
    try {
      FileUtils.deleteDirectory(dataBasePath);
      DaVinciClient<Long, GenericRecord> client = getGenericAvroDaVinciClient(storeName, zkAddress, dataBasePath.toString());
      // Ingest data to local folder.
      client.subscribeAll().get(60, TimeUnit.SECONDS);
      client.close();
    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
      throw new VeniceException(e);
    }
  }

  private void parseClusterInfoFile() {
    try (BufferedReader reader = new BufferedReader(new FileReader(clusterInfoFilePath))) {
      String line = reader.readLine();
      String[] strings = line.split(" ");
      Assert.assertEquals(strings[0], "storeName");
      storeName = strings[1];
      line = reader.readLine();
      strings = line.split(" ");
      Assert.assertEquals(strings[0], "zkAddress");
      zkAddress = strings[1];
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
        .include(IngestionBenchmarkWithTwoProcesses.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }
}
