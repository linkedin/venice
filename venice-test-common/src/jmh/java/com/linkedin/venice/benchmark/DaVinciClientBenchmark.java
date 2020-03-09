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
  private DaVinciClient<Object, Object> client;
  private GenericRecord record;

  @Param({"1", "10", "100"})
  protected String valueLength;

  @Param({"FLOAT_VECTOR", "SPARSE_VECTOR"})
  protected String valueType;

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
    client.subscribeToAllPartitions().get(30, TimeUnit.SECONDS);
  }

  @TearDown
  public void teardown() throws InterruptedException {
    client.close();
    cluster.close();
  }

  @Benchmark
  public void randomReadQueries() throws ExecutionException, InterruptedException {
    long key = 11;
    for (int i = 0; i < queryCount; ++i) {
      GenericRecord value = (GenericRecord)(client.get(key).get());
      Assert.assertEquals(Integer.parseInt(valueLength), ((List<Float>)value.get("value")).size());
      key = (key * 31) % recordCount;
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(DaVinciClientBenchmark.class.getSimpleName()).build();
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