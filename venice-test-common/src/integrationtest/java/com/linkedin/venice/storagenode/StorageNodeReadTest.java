package com.linkedin.venice.storagenode;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.router.api.VenicePathParser.*;


@Test(singleThreaded = true)
public class StorageNodeReadTest {
  private static final Logger LOGGER = Logger.getLogger(StorageNodeReadTest.class);

  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private int partitionCount;

  private String serverAddr;
  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  private VeniceWriter<Object, Object, Object> veniceWriter;
  private AvroGenericStoreClient client;

  private final Base64.Encoder encoder = Base64.getUrlEncoder();

  @BeforeClass(alwaysRun = true)
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    boolean enableSSL = false;
    veniceCluster = ServiceFactory.getVeniceCluster(enableSSL);
    serverAddr = veniceCluster.getVeniceServers().get(0).getAddress();
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
    partitionCount = creationResponse.getPartitions();


    // TODO: Make serializers parameterized so we test them all.
    String stringSchema = "\"string\"";
    keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    valueSerializer = new VeniceAvroKafkaSerializer(stringSchema);

    veniceWriter = TestUtils.getVeniceWriterFactory(veniceCluster.getKafka().getAddress())
        .createVeniceWriter(storeVersionName, keySerializer, valueSerializer);
    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(veniceCluster.getRandomRouterURL()));

  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(client);
    IOUtils.closeQuietly(veniceWriter);
    IOUtils.closeQuietly(veniceCluster);
  }

  private int getPartitionId(byte[] key) {
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    return partitioner.getPartitionId(key, partitionCount);
  }

  /**
   * There are (at least) two types of flakiness in this test:
   *
   * 1. Sometimes, it times out. Usually, the test completes in under 3 sec, so the 10s timeout should be generous...
   *    At this point, it is unclear why the test times out. TODO: Debug the timeout.
   * 2. Sometimes, data is apparently not fully ingested in all partitions by the time the batch get happens,
   *    so the offset data is still at zero... This shouldn't happen, since data freshness is checked in
   *    {@link #pushSyntheticData(String, String, int, VeniceClusterWrapper, VeniceWriter, int)} so it is not
   *    clear why that would be the case. TODO: Debug why the offset occasionally shows as zero
   */
  @Test(timeOut = 10 * Time.MS_PER_SECOND, groups = {"flaky"})
  public void testRead() throws Exception {
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    String keyPrefix = "key_";
    String valuePrefix = "value_";
    pushSyntheticData(keyPrefix, valuePrefix, 100, veniceCluster, veniceWriter, pushVersion);

    try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {
      client.start();

      // Single get
      byte[] keyBytes = keySerializer.serialize(null, keyPrefix + "0");
      StringBuilder sb = new StringBuilder().append("http://")
          .append(serverAddr)
          .append("/")
          .append(TYPE_STORAGE)
          .append("/")
          .append(storeVersionName)
          .append("/")
          .append(getPartitionId(keyBytes))
          .append("/")
          .append(encoder.encodeToString(keyBytes))
          .append("?f=b64");
      HttpGet getReq = new HttpGet(sb.toString());
      Future<HttpResponse> future = client.execute(getReq, null);
      HttpResponse response = future.get();
      try (InputStream bodyStream = response.getEntity().getContent()) {
        byte[] body = IOUtils.toByteArray(bodyStream);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
            "Response did not return 200: " + new String(body));
        Object value = valueSerializer.deserialize(null, body);
        Assert.assertEquals(value.toString(), valuePrefix + "0");
      }

      // Multi-get
      List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>();
      Set<Integer> partitionIdSet = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
        keyBytes = keySerializer.serialize(null, keyPrefix + i);
        requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
        requestKey.keyIndex = i;
        requestKey.partitionId = getPartitionId(keyBytes);
        partitionIdSet.add(requestKey.partitionId);
        keys.add(requestKey);
      }
      RecordSerializer<MultiGetRouterRequestKeyV1> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
      byte[] postBody = serializer.serializeObjects(keys);
      StringBuilder multiGetUri = new StringBuilder().append("http://")
          .append(serverAddr)
          .append("/")
          .append(TYPE_STORAGE)
          .append("/")
          .append(storeVersionName);
      HttpPost httpPost = new HttpPost(multiGetUri.toString());
      BasicHttpEntity entity = new BasicHttpEntity();
      entity.setContent(new ByteArrayInputStream(postBody));
      httpPost.setEntity(entity);
      httpPost.setHeader(HttpConstants.VENICE_API_VERSION,
          Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));

      RecordDeserializer<MultiGetResponseRecordV1> deserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);

      Future<HttpResponse> multiGetFuture = client.execute(httpPost, null);
      HttpResponse multiGetResponse = multiGetFuture.get();
      try (InputStream bodyStream = multiGetResponse.getEntity().getContent()) {
        byte[] body = IOUtils.toByteArray(bodyStream);
        Assert.assertEquals(multiGetResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK,
            "Response did not return 200: " + new String(body));
        /**
         * Validate header: {@link HttpConstants.VENICE_OFFSET}
         */
        Header partitionOffsetHeader = multiGetResponse.getFirstHeader(HttpConstants.VENICE_OFFSET);
        Assert.assertNotNull(partitionOffsetHeader);
        String headerValue = partitionOffsetHeader.getValue();
        Map<Integer, Long> partitionOffsetMap = PartitionOffsetMapUtils.deserializePartitionOffsetMap(headerValue);
        partitionIdSet.forEach( partitionId -> {
          Long offset = partitionOffsetMap.get(partitionId);
          Assert.assertNotNull(offset);
          // TODO: Figure out why the assertion below occasionally fails...
          Assert.assertTrue(offset > 0, "Offset <= 0 for partition '" + partitionId + "'.");
        });

        Iterable<MultiGetResponseRecordV1> values = deserializer.deserializeObjects(body);
        Map<Integer, String> results = new HashMap<>();
        values.forEach(K -> {
          Object value = valueSerializer.deserialize(null, K.value.array());
          results.put(K.keyIndex, value.toString());
        });
        Assert.assertEquals(results.size(), 10);
        for (int i = 0; i < 10; ++i) {
          Assert.assertEquals(results.get(i), valuePrefix + i);
        }
      }
    }

    /**
     * Test with {@link AvroGenericStoreClient}.
     */
    try (AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr))) {
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      Map<String, CharSequence> result = storeClient.batchGet(keySet).get();
      Assert.assertEquals(result.size(), 10);
      for (int i = 0; i < 10; ++i) {
        Assert.assertEquals(result.get(keyPrefix + i).toString(), valuePrefix + i);
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDiskHealthCheckService() throws Exception  {
    VeniceServerWrapper serverWrapper = null;
    try {
      Properties serverProperties = new Properties();
      serverProperties.put(SERVER_DISK_HEALTH_CHECK_INTERVAL_IN_SECONDS, 5); // set health check interval to 10 seconds
      serverProperties.put(SERVER_SHUTDOWN_DISK_UNHEALTHY_TIME_MS, 1000); // set health check ssd shutdown to 1 second
      serverWrapper = veniceCluster.addVeniceServer(serverProperties);
      String testServerAddr = serverWrapper.getAddress();

      try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {
        client.start();

        HttpResponse response = sendHeartbeatRequest(client, testServerAddr);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);

        // wait for the next health check cycle
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        response = sendHeartbeatRequest(client, testServerAddr);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);

        // delete the db path
        FileUtils.deleteDirectory(serverWrapper.getDataDirectory());

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        response = sendHeartbeatRequest(client, testServerAddr);

        Assert.assertEquals(response.getStatusLine().getStatusCode(), 500);
      }
    } finally {
      if (serverWrapper != null) {
        /**
         * Stop and remove the new server from the test cluster after this unit test.
         */
        veniceCluster.removeVeniceServer(serverWrapper.getPort());
      }
    }
  }

  private HttpResponse sendHeartbeatRequest(CloseableHttpAsyncClient client, String serverAddress) throws Exception {
    StringBuilder sb = new StringBuilder().append("http://")
        .append(serverAddress)
        .append("/")
        .append(QueryAction.HEALTH.toString().toLowerCase())
        .append("?f=b64");
    HttpGet getReq = new HttpGet(sb.toString());
    Future<HttpResponse> future = client.execute(getReq, null);
    return future.get();
  }

  private void pushSyntheticData(String keyPrefix, String valuePrefix, int numOfRecords,
      VeniceClusterWrapper veniceCluster, VeniceWriter<Object, Object, Object> veniceWriter, int pushVersion)
      throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      veniceWriter.put(keyPrefix + i, valuePrefix + i, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      Assert.assertEquals(currentVersion, pushVersion, "The new version is not activated yet!");
      for (int i = 0; i < numOfRecords; ++i) {
        String key = keyPrefix + i;
        String value = null;
        try {
          value = client.get(key).get().toString();
        } catch (Exception e) {
          LOGGER.error("Caught exception while trying to get data from the store", e);
          Assert.fail("Caught exception while trying to get data from the store: " + e.getMessage());
        }
        Assert.assertNotNull(value, "Key '" + key + "' is not in the store yet.");
        Assert.assertEquals(value, valuePrefix + i, "Key '" + key + "' does not have the right value.");
      }
    });
  }
}
