package com.linkedin.venice.benchmark;

import static com.linkedin.venice.integration.utils.ServiceFactory.getVeniceCluster;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork(value = 2, jvmArgs = { "-Xms4G", "-Xmx4G" })
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class VeniceClientBenchmark {
  protected static final int KEY_COUNT = 100_000;
  protected static final int RECORD_COUNT = 10_000;
  protected static final String VALUE_FIELD_NAME = "value";
  protected int[] keys = new int[KEY_COUNT];

  protected VeniceClusterWrapper cluster;
  protected AvroGenericStoreClient client;

  @Setup
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    cluster = getVeniceCluster(1, 1, 1);
    String storeName = buildStore(cluster);
    cluster.useControllerClient(c -> c.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(10000)));
    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()));

    Random random = ThreadLocalRandom.current();
    for (int i = 0; i < KEY_COUNT; ++i) {
      keys[i] = random.nextInt(RECORD_COUNT);
    }

    // JMH benchmark relies on System.exit to finish one round of benchmark run, otherwise it will hang there.
    TestUtils.restoreSystemExit();
  }

  @TearDown
  public void cleanUp() {
    client.close();
    cluster.close();
  }

  protected String buildStore(VeniceClusterWrapper cluster) throws Exception {
    Schema schema = Schema.parse(
        "{" + "  \"namespace\" : \"example.avro\"," + "  \"type\": \"record\"," + "  \"name\": \"DenseVector\","
            + "  \"fields\": [" + "     { \"name\": \"value\", \"type\": {\"type\": \"array\", \"items\": \"float\"} }"
            + "   ]" + "}");
    GenericRecord record = new GenericData.Record(schema);
    List<Float> values = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      values.add(1.0f * i);
    }
    record.put(VALUE_FIELD_NAME, values);
    return cluster.createStore(RECORD_COUNT, record);
  }

  public static void main(String[] args) throws Exception {
    Options opt =
        new OptionsBuilder().include(VeniceClientBenchmark.class.getSimpleName()).addProfiler(GCProfiler.class).build();
    new Runner(opt).run();
  }

  @Benchmark
  @OperationsPerInvocation(KEY_COUNT)
  public void runAvroClientQueries(Blackhole blackhole) throws ExecutionException, InterruptedException {
    for (int i = 0; i < KEY_COUNT; ++i) {
      blackhole.consume(client.get(keys[i]).get());
    }
  }
}
