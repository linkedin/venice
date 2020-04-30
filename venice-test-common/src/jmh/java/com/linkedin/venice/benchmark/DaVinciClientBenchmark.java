package com.linkedin.venice.benchmark;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.Assert;

import static com.linkedin.venice.integration.utils.ServiceFactory.*;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class DaVinciClientBenchmark {
  private static final Logger logger = Logger.getLogger(DaVinciClientBenchmark.class);
  private static final int recordCount = 100_000;
  private static final int queryCount = 1000_000;
  private static final String FLOAT_VECTOR_VALUE_SCHEMA = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"FloatVector\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }  " +
      "  ] " +
      " } ";

  private static final String SPARSE_VECTOR_VALUE_SCHEMA = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"SparseVector\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"index\", \"type\": {\"type\": \"array\", \"items\": \"int\"} },  " +
      "       { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }  " +

      "  ] " +
      " } ";

  private VeniceClusterWrapper cluster;
  private DaVinciClient<Long, GenericRecord> client;
  private GenericRecord record;
  private long[] keys = new long[queryCount];

  @Param({"1", "10", "100"})
//  @Param({"2500"})
  protected String valueLength;

  @Param({"FLOAT_VECTOR", "SPARSE_VECTOR"})
//  @Param({"FLOAT_VECTOR"})
  protected String valueType;

  @Param({"true"})
//  @Param({"true", "false"})
  protected boolean assertions;

  @Setup
  public void setup() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 1, 1);

    String storeName;
    if (valueType.equals("FLOAT_VECTOR")) {
      storeName = buildFloatVectorStore(cluster, Integer.parseInt(valueLength));
    } else if (valueType.equals("SPARSE_VECTOR")) {
      storeName = buildSparseVectorStore(cluster, Integer.parseInt(valueLength));
    } else {
      throw new VeniceException("Value type " + valueType + " is not supported in benchmark.");
    }

    client = getGenericAvroDaVinciClient(storeName, cluster);
    client.subscribeToAllPartitions().get(60, TimeUnit.SECONDS);

    // Trying to close as much as possible of the stuff we don't need to minimize interference...
    cluster.getVeniceServers().forEach(veniceServerWrapper -> veniceServerWrapper.close());
    cluster.getVeniceRouters().forEach(veniceRouterWrapper -> veniceRouterWrapper.close());
    cluster.getVeniceControllers().forEach(veniceControllerWrapper -> veniceControllerWrapper.close());

    long key = 11;
    for (int i = 0; i < queryCount; ++i) {
      keys[i] = (key * 31) % recordCount;
    }
  }

  @TearDown
  public void teardown() throws InterruptedException {
    client.close();
    cluster.close();
  }

  @Benchmark
  @OperationsPerInvocation(queryCount)
  public void randomReadQueries() throws ExecutionException, InterruptedException {
    GenericRecord value = null;
    int intValueLength = Integer.parseInt(valueLength);
    String fieldName = "value";
    for (int i = 0; i < queryCount; ++i) {
      value = client.get(keys[i], value).get();
      if (assertions) {
        Assert.assertEquals(((List<Float>)value.get(fieldName)).size(), intValueLength);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(DaVinciClientBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }

  private String buildFloatVectorStore(VeniceClusterWrapper cluster, int valueSize) throws Exception {
    Schema schema = Schema.parse(FLOAT_VECTOR_VALUE_SCHEMA);
    record = new GenericData.Record(schema);
    List<Float> floatVector = new ArrayList<>();
    for (int i = 0; i < valueSize; i++) {
      floatVector.add((float)(i * 1.0));
    }
    record.put("value", floatVector);

    return cluster.createStore(recordCount, record);
  }


  private String buildSparseVectorStore(VeniceClusterWrapper cluster, int valueSize) throws Exception {
    Schema schema = Schema.parse(SPARSE_VECTOR_VALUE_SCHEMA);
    record = new GenericData.Record(schema);
    List<Integer> sparseVectorIndies = new ArrayList<>();
    List<Float> sparseVectorValues = new ArrayList<>();
    for (int i = 0; i < valueSize; i++) {
      sparseVectorIndies.add(i);
      sparseVectorValues.add((float)(i * 1.0));
    }
    record.put("index", sparseVectorIndies);
    record.put("value", sparseVectorValues);

    return cluster.createStore(recordCount, record);
  }
}