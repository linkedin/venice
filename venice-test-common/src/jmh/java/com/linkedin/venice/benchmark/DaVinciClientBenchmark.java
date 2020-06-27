package com.linkedin.venice.benchmark;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.Utils;

import com.linkedin.davinci.client.DaVinciClient;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.integration.utils.ServiceFactory.*;


@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DaVinciClientBenchmark {
  protected static final int RECORD_COUNT = 100_000;
  protected static final int KEY_COUNT = 1000_000;
  protected static final String VALUE_FIELD_NAME = "value";

  @Param({"DENSE_VECTOR"})
  protected String valueType;

  @Param({"2500"})
  protected int valueLength;

  protected VeniceClusterWrapper cluster;
  protected DaVinciClient<Integer, GenericRecord> client;
  protected int[] keys = new int[KEY_COUNT];

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
                      .include(DaVinciClientBenchmark.class.getSimpleName())
                      .addProfiler(GCProfiler.class)
                      .build();
    new Runner(opt).run();
  }

  @Setup
  public void setup() throws Exception {
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
    client.subscribeToAllPartitions().get(60, TimeUnit.SECONDS);

    // Close as much as possible of the stuff we don't need, to minimize interference.
    cluster.getVeniceServers().forEach(VeniceServerWrapper::close);
    cluster.getVeniceRouters().forEach(VeniceRouterWrapper::close);
    cluster.getVeniceControllers().forEach(VeniceControllerWrapper::close);

    Random random = new Random(11);
    for (int i = 0; i < KEY_COUNT; ++i) {
      keys[i] = random.nextInt(RECORD_COUNT);
    }
  }

  @TearDown
  public void cleanup() {
    client.close();
    cluster.close();
  }

  @Benchmark
  @Threads(1)
  @OperationsPerInvocation(KEY_COUNT)
  public void testSingleGetHitT1(Blackhole blackhole) throws Exception {
    runSingleGetHitWorkload(blackhole);
  }

  @Benchmark
  @Threads(8)
  @OperationsPerInvocation(KEY_COUNT)
  public void testSingleGetHitT8(Blackhole blackhole) throws Exception {
    runSingleGetHitWorkload(blackhole);
  }

  protected void runSingleGetHitWorkload(Blackhole blackhole) throws Exception {
    GenericRecord record = null;
    for (int i = 0; i < KEY_COUNT; ++i) {
      record = client.get(keys[i], record).get();
      blackhole.consume(record);
    }
  }

  protected String buildDenseVectorStore(VeniceClusterWrapper cluster) throws Exception {
    Schema schema = Schema.parse(
            "{" +
            "  \"namespace\" : \"example.avro\"," +
            "  \"type\": \"record\"," +
            "  \"name\": \"DenseVector\"," +
            "  \"fields\": [" +
            "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }" +
            "   ]" +
            "}");
    GenericRecord record = new GenericData.Record(schema);
    List<Float> values = new ArrayList<>();
    for (int i = 0; i < valueLength; i++) {
      values.add(1.0f * i);
    }
    record.put(VALUE_FIELD_NAME, values);
    return cluster.createStore(RECORD_COUNT, record);
  }


  protected String buildSparseVectorStore(VeniceClusterWrapper cluster) throws Exception {
    Schema schema = Schema.parse(
            "{" +
            "  \"namespace\" : \"example.avro\"," +
            "  \"type\": \"record\"," +
            "  \"name\": \"SparseVector\"," +
            "  \"fields\": [" +
            "     { \"name\": \"index\", \"type\": {\"type\": \"array\", \"items\": \"int\"} }," +
            "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }" +
            "   ]" +
            "}");
    GenericRecord record = new GenericData.Record(schema);
    List<Integer> indices = new ArrayList<>();
    List<Float> values = new ArrayList<>();
    for (int i = 0; i < valueLength; i++) {
      indices.add(i);
      values.add(1.0f * i);
    }
    record.put("index", indices);
    record.put(VALUE_FIELD_NAME, values);
    return cluster.createStore(RECORD_COUNT, record);
  }
}
