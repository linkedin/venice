package com.linkedin.venice.router;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

public class ReplicaFailoverTest {
  private final int KEY_COUNT = 1000;
  private final int TEST_TIMEOUT = 15000; // ms

  private VeniceClusterWrapper cluster;
  private String storeName;

  @BeforeClass
  void setup() throws InterruptedException, ExecutionException, VeniceClientException {
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2);

    String keySchema = "\"string\"";
    String valueSchema = "{\"type\": \"record\", \"name\": \"dummy_schema\", \"fields\": []}";
    VersionCreationResponse response = cluster.getNewStoreVersion(keySchema, valueSchema);
    storeName = Version.parseStoreFromKafkaTopicName(response.getKafkaTopic());

    VeniceTestWriterFactory writerFactory = TestUtils.getVeniceTestWriterFactory(this.cluster.getKafka().getAddress());
    try (
        VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
        VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchema);
        VeniceWriter<Object, Object> writer = writerFactory.getVeniceWriter(response.getKafkaTopic(), keySerializer, valueSerializer)) {

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
  void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @BeforeMethod
  void setupTestCase() {
    // remove all server hooks
    for (VeniceServerWrapper server : cluster.getVeniceServers()) {
      server.getVeniceServer().setRequestHandler(null);
    }

    // restart routers to discard server health info
    for (VeniceRouterWrapper router : cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  void testDeadReplica() throws Exception {
    VeniceServerWrapper server = cluster.getVeniceServers().get(1);
    cluster.stopVeniceServer(server.getPort());
    runWorkload(90.0, 100);
    cluster.restartVeniceServer(server.getPort());
  }

  @Test(timeOut = TEST_TIMEOUT, enabled = false)
  void testHungReplica() throws Exception {
    AtomicInteger hitCount = new AtomicInteger();
    VeniceServerWrapper server = cluster.getVeniceServers().get(0);
    server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
      if (!(msg instanceof RouterRequest)) { // passthrough non data requests
        return false;
      }
      hitCount.incrementAndGet();
      return true; // drop all data requests
    });
    runWorkload(90.0, 100);
    Assert.assertNotEquals(hitCount.get(), 0);
  }

  @Test(timeOut = TEST_TIMEOUT, enabled = false)
  void testSlowReplica() throws Exception {
    AtomicInteger hitCount = new AtomicInteger();
    VeniceServerWrapper server = cluster.getVeniceServers().get(0);
    server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
      hitCount.incrementAndGet();
      Utils.sleep(500); // delay all requests by 0.5sec
      return false;
    });
    runWorkload(90.0, 100);
    Assert.assertNotEquals(hitCount.get(), 0);
  }

  @Test(timeOut = TEST_TIMEOUT)
  void testFaultyReplica() throws Exception {
    AtomicInteger hitCount = new AtomicInteger();
    VeniceServerWrapper server = cluster.getVeniceServers().get(0);
    server.getVeniceServer().setRequestHandler((ChannelHandlerContext ctx, Object msg) -> {
      if (!(msg instanceof RouterRequest)) { // passthrough non data requests
        return false;
      }
      hitCount.incrementAndGet();
      ctx.writeAndFlush(new HttpShortcutResponse("", HttpResponseStatus.INTERNAL_SERVER_ERROR));
      return true;
    });
    runWorkload(90.0, 100);
    Assert.assertNotEquals(hitCount.get(), 0);
  }

  void runWorkload(double percentile, long maxLatency) throws Exception {
    ClientConfig config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL());
    try (AvroGenericStoreClient<String, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(config)) {
      int outliers = 0;
      for (int i = 0; i < KEY_COUNT; ++i) {
        long startTime = System.nanoTime();
        client.get(String.valueOf(i)).get();
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        if (latency > maxLatency && ++outliers > KEY_COUNT * (1 - percentile / 100.0)) {
          Assert.fail(outliers + " out of " + KEY_COUNT + " requests exceeded " + maxLatency + "ms, last latency is " + latency + "ms");
        }
      }
    }
  }
}
