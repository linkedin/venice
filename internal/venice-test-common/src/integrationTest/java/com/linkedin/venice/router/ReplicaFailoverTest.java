package com.linkedin.venice.router;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ReplicaFailoverTest {
  private static final Logger LOGGER = LogManager.getLogger(ReplicaFailoverTest.class);

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
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, REPLICATION_FACTOR, 0, REPLICATION_FACTOR);
    Properties props = new Properties();
    props.setProperty(ConfigKeys.ROUTER_STATEFUL_HEALTHCHECK_ENABLED, "true");
    props.setProperty(ConfigKeys.ROUTER_ASYNC_START_ENABLED, "true");
    props.setProperty(
        ConfigKeys.ROUTER_UNHEALTHY_PENDING_CONNECTION_THRESHOLD_PER_ROUTE,
        String.valueOf(MAX_CONCURRENT_REQUESTS));
    props.setProperty(
        ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS,
        String.valueOf(MAX_REQUEST_LATENCY_QD1 / 2));
    cluster.addVeniceRouter(props);
    storeName = cluster.createStore(KEY_COUNT);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @BeforeMethod
  public void setupTestCase() {
    // remove all server hooks
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      server.getVeniceServer().setRequestHandler(null);
    }
    errorHitCountMap.clear();

    // restart routers to discard server health info
    for (VeniceRouterWrapper router: cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }

  @DataProvider(name = "workloadParams")
  public static Object[][] workloadParams() {
    return new Object[][] { { 1 }, { MAX_CONCURRENT_REQUESTS } };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testDeadReplica(int maxConcurrentRequests) {
    List<VeniceServerWrapper> servers = cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1);
    for (VeniceServerWrapper server: servers) {
      cluster.stopVeniceServer(server.getPort());
    }

    runWorkload(maxConcurrentRequests, REQUEST_PERCENTILE, MAX_REQUEST_LATENCY_QD1);

    for (VeniceServerWrapper server: servers) {
      cluster.restartVeniceServer(server.getPort());
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "workloadParams")
  public void testDisconnectedReplica(int maxConcurrentRequests) {
    for (VeniceServerWrapper server: cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
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
    for (VeniceServerWrapper server: cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
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
    for (VeniceServerWrapper server: cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
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
    for (VeniceServerWrapper server: cluster.getVeniceServers().subList(0, REPLICATION_FACTOR - 1)) {
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

    ClientConfig config = ClientConfig.defaultGenericClientConfig(storeName)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setVeniceURL(cluster.getZk().getAddress());

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(config)) {

      Semaphore semaphore = new Semaphore(maxConcurrentRequests);
      AtomicReference<Throwable> lastFailure = new AtomicReference<>();

      while (submittedRequests < KEY_COUNT) {
        Assert.assertTrue(
            semaphore.tryAcquire(1, getMaxTimeToCompleteRequests(1), TimeUnit.MILLISECONDS),
            "Minimal QPS requirement not met");

        long startTime = System.nanoTime();
        client.get(submittedRequests).whenComplete((value, throwable) -> {
          if (throwable != null) {
            lastFailure.set(throwable);
          }

          if (!Objects.equals(value, 1)) {
            lastFailure.set(new AssertionError("Unexpected single-get result, expected=" + 1 + ", actual=" + value));
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
          Assert.assertTrue(
              semaphore.tryAcquire(
                  maxConcurrentRequests,
                  getMaxTimeToCompleteRequests(maxConcurrentRequests),
                  TimeUnit.MILLISECONDS),
              "Minimal QPS requirement not met");
        }

        if (outlierRequests.get() > KEY_COUNT * (1 - requestPercentile / 100.0)) {
          Assert.fail(
              outlierRequests.get() + " out of " + completedRequests.get() + " single-get requests exceeded "
                  + maxConcurrentLatency + " ms, " + "average outlier latency is "
                  + totalOutlierLatency.get() / Math.max(1, outlierRequests.get()) + " ms, " + "average latency is "
                  + totalLatency.get() / Math.max(1, completedRequests.get()) + " ms");
        }
      }

      for (AtomicInteger hitCount: errorHitCountMap.values()) {
        Assert.assertNotEquals(hitCount.get(), 0);
      }

    } catch (InterruptedException e) {
      throw new AssertionError(e);

    } finally {
      LOGGER.info(
          "{} out of {} single-get requests exceeded {} ms, average outlier latency is {} ms, average latency is {} ms",
          outlierRequests.get(),
          completedRequests.get(),
          maxConcurrentLatency,
          totalOutlierLatency.get() / Math.max(1, outlierRequests.get()),
          totalLatency.get() / Math.max(1, completedRequests.get()));

      if (!errorHitCountMap.isEmpty()) {
        LOGGER.info(
            errorHitCountMap.values().stream().mapToInt(AtomicInteger::get).sum() + " errors were triggered in total");
      }
    }
  }
}
