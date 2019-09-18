package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestUtils.VeniceTestWriterFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.Assert;

public class ReplicaFailoverTest {
  private static final Logger logger = Logger.getLogger(ReplicaFailoverTest.class);

  private static final int KEY_COUNT = 1000;
  private static final int TEST_TIMEOUT = 15000; // ms
  private static final int REPLICATION_FACTOR = 2;

  private static final int MAX_CONCURRENT_REQUESTS = 20;
  private static final double REQUEST_PERCENTILE = 90.0;
  private static final long MAX_REQUEST_LATENCY_QD1 = 50; // ms
  private static final double MIN_QPS = 1; // requests per second

  private VeniceClusterWrapper cluster;
  private String storeName;
  private Map<Integer, AtomicInteger> errorHitCountMap = new HashMap<>();

  @BeforeClass
  public void setup() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, REPLICATION_FACTOR, 0, REPLICATION_FACTOR);
    Properties props = new Properties();
    props.setProperty(ConfigKeys.ROUTER_STATEFUL_HEALTHCHECK_ENABLED, "true");
    props.setProperty(ConfigKeys.ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE, String.valueOf(MAX_CONCURRENT_REQUESTS));
    props.setProperty(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, String.valueOf(MAX_REQUEST_LATENCY_QD1 / 2));
    cluster.addVeniceRouter(props);

    String keySchema = "\"string\"";
    String valueSchema = "{\"type\": \"record\", \"name\": \"dummy_schema\", \"fields\": []}";
    VersionCreationResponse response = cluster.getNewStoreVersion(keySchema, valueSchema);
    storeName = Version.parseStoreFromKafkaTopicName(response.getKafkaTopic());

    VeniceTestWriterFactory writerFactory = TestUtils.getVeniceTestWriterFactory(this.cluster.getKafka().getAddress());
    try (
        VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchema);
        VeniceWriter<Object, Object, byte[]> writer = writerFactory.getVeniceWriter(response.getKafkaTopic(), keySerializer, valueSerializer)) {

      GenericRecord record = new GenericData.Record(new Schema.Parser().parse(valueSchema));
      int valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

      writer.broadcastStartOfPush(new HashMap<>());
      for (int i = 0; i < KEY_COUNT; ++i) {
        writer.put(String.valueOf(i), record, valueSchemaId).get();
      }
      writer.broadcastEndOfPush(new HashMap<>());
    }

    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      if (ControllerClient.getStore(cluster.getAllControllersURLs(), cluster.getClusterName(), storeName).getStore().getCurrentVersion() == 0) {
        return false;
      }
      cluster.refreshAllRouterMetaData();
      return true;
    });
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @BeforeMethod
  public void setupTestCase() {
    // remove all server hooks
    for (VeniceServerWrapper server : cluster.getVeniceServers()) {
      server.getVeniceServer().setRequestHandler(null);
    }
    errorHitCountMap.clear();

    // restart routers to discard server health info
    for (VeniceRouterWrapper router : cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }

  @DataProvider(name = "workloadParams")
  public static Object[][] workloadParams() {
    return new Object[][]{{1}, {MAX_CONCURRENT_REQUESTS}};
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  void testDeadReplica(int maxConcurrentRequests) {
    List<VeniceServerWrapper> servers = cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1);
    for (VeniceServerWrapper server : servers) {
      cluster.stopVeniceServer(server.getPort());
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);

    for (VeniceServerWrapper server : servers) {
      cluster.restartVeniceServer(server.getPort());
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testDisconnectedReplica(int maxConcurrentRequests) {
    for (VeniceServerWrapper server : cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
      AtomicInteger hitCount = new AtomicInteger();
      errorHitCountMap.put(server.getPort(), hitCount);

      server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
        hitCount.incrementAndGet();
        ctx.close(); // close the connection
        return true;
      });
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testHungReplica(int maxConcurrentRequests) {
    for (VeniceServerWrapper server : cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
      AtomicInteger hitCount = new AtomicInteger();
      errorHitCountMap.put(server.getPort(), hitCount);

      server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
        if (!(msg instanceof RouterRequest)) { // passthrough non data requests
          return false;
        }
        hitCount.incrementAndGet();
        return true; // drop all data requests
      });
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testSlowReplica(int maxConcurrentRequests) {
    for (VeniceServerWrapper server : cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
      AtomicInteger hitCount = new AtomicInteger();
      errorHitCountMap.put(server.getPort(), hitCount);

      server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
        hitCount.incrementAndGet();
        Utils.sleep(500); // delay all requests by 0.5sec
        return false;
      });
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testFaultyReplica(int maxConcurrentRequests) {
    for (VeniceServerWrapper server : cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
      AtomicInteger hitCount = new AtomicInteger();
      errorHitCountMap.put(server.getPort(), hitCount);

      server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
        if (!(msg instanceof RouterRequest)) { // pass-through non data requests
          return false;
        }
        hitCount.incrementAndGet();
        ctx.writeAndFlush(new HttpShortcutResponse("", HttpResponseStatus.INTERNAL_SERVER_ERROR));
        return true;
      });
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);
  }

  long getMaxTimeToCompleteRequests(int requestCount) {
    return Math.round(1000. * requestCount / MIN_QPS);
  }

  void runWorkload(int maxConcurrentRequests, double requestPercentile, long maxLatency) {
    // in worst case, server handles one request at a time, hence the multiplication
    final long maxConcurrentLatency = maxLatency * maxConcurrentRequests;

    int submittedRequests = 0;
    AtomicInteger outlierRequests = new AtomicInteger();
    AtomicInteger completedRequests = new AtomicInteger();
    AtomicLong totalLatency = new AtomicLong();
    AtomicLong totalOutlierLatency = new AtomicLong();

    ClientConfig config = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setVeniceURL(cluster.getZk().getAddress());

    try (AvroGenericStoreClient<String, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(config)) {

      Semaphore semaphore = new Semaphore(maxConcurrentRequests);
      AtomicReference<Throwable> lastFailure = new AtomicReference<>();

      while (submittedRequests < KEY_COUNT) {
        Assert.assertTrue(semaphore.tryAcquire(1, getMaxTimeToCompleteRequests(1), TimeUnit.MILLISECONDS),
            "Minimal QPS requirement not met");

        long startTime = System.nanoTime();
        client.get(String.valueOf(submittedRequests)).whenComplete((record, throwable) -> {
          if (throwable != null) {
            lastFailure.set(throwable);
          }

          long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
          completedRequests.incrementAndGet();
          totalLatency.addAndGet(latency);

          if (latency > maxConcurrentLatency) {
            outlierRequests.incrementAndGet();
            totalOutlierLatency.addAndGet(latency);
          }

          semaphore.release();
        });

        ++submittedRequests;
        if (lastFailure.get() != null) {
          throw new AssertionError(lastFailure.get());
        }

        if (submittedRequests == KEY_COUNT) {
          // drain pending requests, after submitting the last one
          Assert.assertTrue(semaphore.tryAcquire(maxConcurrentRequests, getMaxTimeToCompleteRequests(maxConcurrentRequests), TimeUnit.MILLISECONDS),
              "Minimal QPS requirement not met");
        }

        if (outlierRequests.get() > KEY_COUNT * (1 - requestPercentile / 100.0)) {
          Assert.fail(outlierRequests.get() + " out of " + completedRequests.get() + " single-get requests exceeded " + maxConcurrentLatency + "ms, " +
              "average outlier latency is " + totalOutlierLatency.get() / Math.max(1, outlierRequests.get()) + "ms, " +
              "average latency is " + totalLatency.get() / Math.max(1, completedRequests.get()) + "ms");
        }
      }

      for (AtomicInteger hitCount : errorHitCountMap.values()) {
        Assert.assertNotEquals(hitCount.get(), 0);
      }

    } catch (InterruptedException e) {
      throw new AssertionError(e);

    } finally {
      logger.info(outlierRequests.get() + " out of " + completedRequests.get() + " single-get requests exceeded " + maxConcurrentLatency + "ms, " +
          "average outlier latency is " + totalOutlierLatency.get() / Math.max(1, outlierRequests.get()) + "ms, " +
          "average latency is " + totalLatency.get() / Math.max(1, completedRequests.get()) + "ms");

      if (!errorHitCountMap.isEmpty()) {
        logger.info(errorHitCountMap.values().stream().mapToInt(AtomicInteger::get).sum() + " errors were triggered in total");
      }
    }
  }
}
