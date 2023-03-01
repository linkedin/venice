package com.linkedin.venice.benchmark;

import static com.linkedin.venice.integration.utils.ServiceFactory.getGenericAvroDaVinciClient;
import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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


@Fork(value = 1, warmups = 1, jvmArgs = { "-Xms4G", "-Xmx4G" })
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DaVinciClientBenchmark {
  protected static final int KEY_COUNT = 100_000;
  protected static final String VALUE_FIELD = "value";

  @Param({ "DENSE_VECTOR" })
  protected String valueType;

  @Param({ "2500" })
  protected int valueLength;

  protected VeniceClusterWrapper cluster;
  protected DaVinciClient<Integer, GenericRecord> client;

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder().include(DaVinciClientBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(options).run();
  }

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    cluster = getVeniceCluster(1, 1, 1);

    String storeName;
    if (valueType.equals("DENSE_VECTOR")) {
      storeName = buildDenseVectorStore(cluster);
    } else if (valueType.equals("SPARSE_VECTOR")) {
      storeName = buildSparseVectorStore(cluster);
    } else {
      throw new VeniceException("Value type " + valueType + " is not supported in benchmark.");
    }

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
  public static class ThreadContext {
    public int key;
    public GenericRecord record;

    @Setup(Level.Invocation)
    public void setUp() {
      key = ThreadLocalRandom.current().nextInt(KEY_COUNT);
    }
  }

  @Benchmark
  @Threads(1)
  public void singleGetHitT1(ThreadContext context, Blackhole blackhole) throws Exception {
    singleGetHit(context, blackhole);
  }

  @Benchmark
  @Threads(8)
  public void singleGetHitT8(ThreadContext context, Blackhole blackhole) throws Exception {
    singleGetHit(context, blackhole);
  }

  protected void singleGetHit(ThreadContext context, Blackhole blackhole) throws Exception {
    context.record = client.get(context.key, context.record).get();
    blackhole.consume(context.record);
  }

  protected String buildDenseVectorStore(VeniceClusterWrapper cluster) throws Exception {
    Schema schema = Schema.parse(
        "{" + "  \"namespace\" : \"example.avro\"," + "  \"type\": \"record\"," + "  \"name\": \"DenseVector\","
            + "  \"fields\": [" + "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }"
            + "   ]" + "}");
    GenericRecord record = new GenericData.Record(schema);
    List<Float> values = new ArrayList<>();
    for (int i = 0; i < valueLength; i++) {
      values.add((float) i);
    }
    record.put(VALUE_FIELD, values);
    return cluster.createStore(KEY_COUNT, record);
  }

  protected String buildSparseVectorStore(VeniceClusterWrapper cluster) throws Exception {
    Schema schema = Schema.parse(
        "{" + "  \"namespace\" : \"example.avro\"," + "  \"type\": \"record\"," + "  \"name\": \"SparseVector\","
            + "  \"fields\": [" + "     { \"name\": \"index\", \"type\": {\"type\": \"array\", \"items\": \"int\"} },"
            + "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }" + "   ]" + "}");
    GenericRecord record = new GenericData.Record(schema);
    List<Integer> indices = new ArrayList<>();
    List<Float> values = new ArrayList<>();
    for (int i = 0; i < valueLength; i++) {
      indices.add(i);
      values.add((float) i);
    }
    record.put("index", indices);
    record.put(VALUE_FIELD, values);
    return cluster.createStore(KEY_COUNT, record);
  }
}
