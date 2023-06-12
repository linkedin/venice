package com.linkedin.venice.benchmark;

import static com.linkedin.venice.integration.utils.ServiceFactory.getGenericAvroDaVinciClient;
import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.Assert;


@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G", "-Djmh.shutdownTimeout=0", "-Djmh.shutdownTimeout.step=0" })
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DaVinciClientBenchmark {
  @Param({ "1000000000" }) // 1GB
  long dataSize;

  @Param({ "10000" }) // 10KB
  int valueSize;

  int keyCount;

  @Param({ "1" })
  int partitionCount;

  VeniceClusterWrapper cluster;
  DaVinciClient<Integer, GenericRecord> client;

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder().include(DaVinciClientBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .shouldFailOnError(true)
        .build();
    new Runner(options).run();
  }

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    cluster = getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().replicationFactor(1)
            .numberOfPartitions(partitionCount)
            .maxNumberOfPartitions(partitionCount)
            .numberOfServers(1)
            .numberOfRouters(1)
            .numberOfControllers(1)
            .build());

    keyCount = (int) (dataSize / valueSize);
    String storeName = buildDenseVectorStore(cluster);
    client = getGenericAvroDaVinciClient(storeName, cluster);
    client.subscribeAll().get(5, TimeUnit.MINUTES);

    // Close as much as possible of the stuff we don't need, to minimize interference.
    cluster.getVeniceRouters().forEach(service -> cluster.removeVeniceRouter(service.getPort()));
    cluster.getVeniceServers().forEach(service -> cluster.removeVeniceServer(service.getPort()));
    cluster.getVeniceControllers().forEach(service -> cluster.removeVeniceController(service.getPort()));

    // JMH benchmark relies on System.exit to finish one round of benchmark run, otherwise it will hang there.
    TestUtils.restoreSystemExit();
  }

  @TearDown
  public void cleanUp() {
    client.close();
    cluster.close();
  }

  @State(Scope.Thread)
  public static class SingleGetThreadContext {
    int key;
    GenericRecord record;

    @Setup(Level.Invocation)
    public void setUp(DaVinciClientBenchmark benchmark) {
      key = ThreadLocalRandom.current().nextInt(benchmark.keyCount);
    }
  }

  @Benchmark
  @Threads(1)
  public void singleGetHitT1(SingleGetThreadContext context, Blackhole blackhole) throws Exception {
    singleGetHit(context, blackhole);
  }

  @Benchmark
  @Threads(1)
  public void singleGetMissT1(SingleGetThreadContext context, Blackhole blackhole) throws Exception {
    singleGetMiss(context, blackhole);
  }

  @Benchmark
  @Threads(8)
  public void singleGetHitT8(SingleGetThreadContext context, Blackhole blackhole) throws Exception {
    singleGetHit(context, blackhole);
  }

  protected void singleGetHit(SingleGetThreadContext context, Blackhole blackhole) throws Exception {
    context.record = client.get(context.key, context.record).get();
    blackhole.consume(context.record);
    Assert.assertNotNull(context.record, "Key=" + context.key);
  }

  protected void singleGetMiss(SingleGetThreadContext context, Blackhole blackhole) throws Exception {
    context.record = client.get(~context.key, context.record).get();
    blackhole.consume(context.record);
    Assert.assertNull(context.record, "Key=" + context.key);
  }

  @State(Scope.Thread)
  public static class BatchGetThreadContext {
    @Param({ "100" })
    int batchGetSize;
    Set<Integer> keys;
    Map<Integer, GenericRecord> result;

    @Setup(Level.Invocation)
    public void setUp(DaVinciClientBenchmark benchmark) {
      keys = ThreadLocalRandom.current()
          .ints(0, benchmark.keyCount)
          .distinct()
          .limit(batchGetSize)
          .boxed()
          .collect(Collectors.toSet());
    }
  }

  @Benchmark
  @Threads(1)
  public void batchGetHitT1(BatchGetThreadContext context, Blackhole blackhole) throws Exception {
    batchGetHit(context, blackhole);
  }

  @Benchmark
  @Threads(8)
  public void batchGetHitT8(BatchGetThreadContext context, Blackhole blackhole) throws Exception {
    batchGetHit(context, blackhole);
  }

  protected void batchGetHit(BatchGetThreadContext context, Blackhole blackhole) throws Exception {
    context.result = client.batchGet(context.keys).get();
    blackhole.consume(context.result);
    Assert.assertEquals(context.result.size(), context.keys.size());
    context.result.values().forEach(Assert::assertNotNull);
  }

  protected String buildDenseVectorStore(VeniceClusterWrapper cluster) {
    Schema schema = AvroCompatibilityHelper.parse(
        "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"DenseVector\", \"fields\": [{\"name\": \"values\", \"type\": {\"type\": \"array\", \"items\": \"float\"}}]}");
    GenericRecord record = new GenericData.Record(schema);
    int length = valueSize / Float.BYTES;
    List<Float> values = new ArrayList<>(length);
    for (int i = 0; i < length; ++i) {
      values.add((float) i);
    }
    record.put("values", values);
    String storeName = cluster.createStore(keyCount, record);
    cluster.createMetaSystemStore(storeName);
    return storeName;
  }
}
