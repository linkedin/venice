package com.linkedin.venice.benchmark;

import static com.linkedin.venice.client.store.predicate.PredicateBuilder.equalTo;
import static com.linkedin.venice.integration.utils.ServiceFactory.getGenericAvroDaVinciClient;
import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
public class DaVinciPartialKeyLookupBenchmark {
  protected static final int KEY_COUNT = 10_000;
  protected static final String VALUE_FIELD = "value";
  protected static final String KEY_PREFIX = "key_";

  @Param({ "2500" })
  protected int valueLength;

  protected VeniceClusterWrapper cluster;
  protected DaVinciClient<GenericRecord, GenericRecord> client;
  protected String keySchemaString = "{" + "  \"namespace\" : \"example.avro\"," + "  \"type\": \"record\","
      + "  \"name\": \"PartialKeyLookup\"," + "  \"fields\": [" + "     { \"name\": \"field1\", \"type\": \"string\" },"
      + "     { \"name\": \"field2\", \"type\": \"string\" }" + "   ]" + "}";

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder().include(DaVinciPartialKeyLookupBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(options).run();
  }

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    cluster = getVeniceCluster(1, 1, 1);

    String storeName = buildVectorStore(cluster);
    client = getGenericAvroDaVinciClient(
        storeName,
        cluster,
        Utils.getTempDataDirectory().getAbsolutePath(),
        new DaVinciConfig().setStorageClass(StorageClass.DISK));
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

  @Benchmark
  @Threads(1)
  public void partialKeyGetHitT1(Blackhole blackhole) {
    Predicate partialKey = equalTo("field1", "key_0");
    client.compute()
        .project("value")
        .executeWithFilter(partialKey, new StreamingCallback<GenericRecord, GenericRecord>() {
          @Override
          public void onRecordReceived(GenericRecord key, GenericRecord value) {
            blackhole.consume(key);
            blackhole.consume(value);
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            exception.ifPresent(Throwable::printStackTrace);
          }
        });
  }

  @Benchmark
  @Threads(1)
  public void batchGetHitT1(Blackhole blackhole) throws ExecutionException, InterruptedException {
    Set<GenericRecord> keys = new HashSet<>();
    Schema keySchema = new Schema.Parser().parse(keySchemaString);

    for (int i = 0; i < KEY_COUNT; i += 100) {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("field1", KEY_PREFIX + "_0");
      key.put("field2", "field_" + i);
      keys.add(key);
    }

    Map<GenericRecord, GenericRecord> results = client.batchGet(keys).get();
    blackhole.consume(results);
  }

  @Benchmark
  @Threads(1)
  public void readComputeHitT1(Blackhole blackhole) throws InterruptedException {
    Set<GenericRecord> keys = new HashSet<>();
    Schema keySchema = new Schema.Parser().parse(keySchemaString);

    for (int i = 0; i < KEY_COUNT; i += 100) {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("field1", KEY_PREFIX + "_0");
      key.put("field2", "field_" + i);
      keys.add(key);
    }

    client.compute()
        .project("value")
        .streamingExecute(keys, new StreamingCallback<GenericRecord, ComputeGenericRecord>() {
          @Override
          public void onRecordReceived(GenericRecord key, ComputeGenericRecord value) {
            blackhole.consume(key);
            blackhole.consume(value);
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            exception.ifPresent(Throwable::printStackTrace);
          }
        });
  }

  protected String buildVectorStore(VeniceClusterWrapper cluster) throws Exception {
    Schema keySchema = new Schema.Parser().parse(keySchemaString);

    String valueSchemaString = "{" + "  \"namespace\" : \"example.avro\"," + "  \"type\": \"record\","
        + "  \"name\": \"DenseVector\"," + "  \"fields\": ["
        + "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }" + "   ]" + "}";
    Schema valueSchema = new Schema.Parser().parse(valueSchemaString);
    String storeName = cluster
        .createStore(keySchemaString, valueSchemaString, generateBatchDataStream(KEY_COUNT, keySchema, valueSchema));

    return storeName;
  }

  private Stream<Map.Entry> generateBatchDataStream(int keyCount, Schema keySchema, Schema valueSchema) {
    Map data = new HashMap<>();
    List<Float> values = new ArrayList<>();
    for (int j = 0; j < valueLength; j++) {
      values.add((float) j);
    }
    for (int i = 0; i < keyCount; ++i) {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("field1", KEY_PREFIX + (i % 100));
      key.put("field2", "field_" + i);

      GenericRecord value = new GenericData.Record(valueSchema);
      value.put(VALUE_FIELD, values);
      data.put(key, value);
    }
    return data.entrySet().stream();
  }
}
