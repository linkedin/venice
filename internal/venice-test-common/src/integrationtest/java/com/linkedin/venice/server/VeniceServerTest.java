package com.linkedin.venice.server;

import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_IS_AUTO_JOIN;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceServerTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerTest.class);

  @Test
  public void testStartServerWithDefaultConfigForTests() throws NoSuchFieldException, IllegalAccessException {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      TestVeniceServer server = cluster.getVeniceServers().get(0).getVeniceServer();
      Assert.assertTrue(server.isStarted());

      Field liveClusterConfigRepoField = server.getClass().getSuperclass().getDeclaredField("liveClusterConfigRepo");
      liveClusterConfigRepoField.setAccessible(true);
      Assert.assertNotNull(liveClusterConfigRepoField.get(server));
    }
  }

  @Test
  public void testStartServerWhenEnableAllowlistCheckingFailed() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Assert.assertThrows(VeniceException.class, () -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(false));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      Assert.assertTrue(cluster.getVeniceServers().isEmpty());
    }
  }

  @Test
  public void testStartServerWhenEnableAllowlistCheckingSuccessful() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
      featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
      cluster.addVeniceServer(featureProperties, new Properties());
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    }
  }

  @Test
  public void testCheckBeforeJoinCluster() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      StorageEngineRepository repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert
          .assertTrue(repository.getAllLocalStorageEngines().isEmpty(), "New node should not have any storage engine.");

      // Create a storage engine.
      String storeName = Utils.getUniqueString("testCheckBeforeJoinCluster");
      server.getVeniceServer()
          .getStorageService()
          .openStoreForNewPartition(
              server.getVeniceServer().getConfigLoader().getStoreConfig(storeName),
              1,
              () -> null);
      Assert.assertEquals(
          repository.getAllLocalStorageEngines().size(),
          1,
          "We have created one storage engine for store: " + storeName);

      // Restart server, as server's info leave in Helix cluster, so we expect that all local storage would NOT be
      // deleted
      // once the server join again.
      cluster.stopVeniceServer(server.getPort());
      cluster.restartVeniceServer(server.getPort());
      repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1, "We should not cleanup the local storage");

      // Stop server, remove it from the cluster then restart. We expect that all local storage would be deleted. Once
      // the server join again.
      cluster.stopVeniceServer(server.getPort());
      try (ControllerClient client = ControllerClient
          .constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
        client.removeNodeFromCluster(Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort()));
      }

      cluster.restartVeniceServer(server.getPort());
      Assert.assertTrue(
          server.getVeniceServer()
              .getStorageService()
              .getStorageEngineRepository()
              .getAllLocalStorageEngines()
              .isEmpty(),
          "After removing the node from cluster, local storage should be cleaned up once the server join the cluster again.");
    }
  }

  @Test
  public void testCheckBeforeJointClusterBeforeHelixInitializingCluster() throws Exception {
    Thread serverAddingThread = null;
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(0, 0, 0)) {
      serverAddingThread = new Thread(() -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(false));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      serverAddingThread.start();

      Utils.sleep(Time.MS_PER_SECOND);
      Assert.assertTrue(
          cluster.getVeniceServers().isEmpty() || !cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
      cluster.addVeniceController(new Properties());

      serverAddingThread.join(30 * Time.MS_PER_SECOND);
      Assert.assertTrue(!serverAddingThread.isAlive(), "Server should be added by now");
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    } finally {
      TestUtils.shutdownThread(serverAddingThread);
    }
  }

  @Test
  public void testMetadataFetchRequest() throws ExecutionException, InterruptedException, IOException {
    Utils.thisIsLocalhost();
    int servers = 6;
    int replicationFactor = 2;

    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, servers, 0, replicationFactor);
        CloseableHttpAsyncClient client =
            HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {

      HelixAdmin admin = new ZKHelixAdmin(cluster.getZk().getAddress());

      HelixConfigScope configScope =
          new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(cluster.getClusterName())
              .build();
      Map<String, String> clusterProperties = new HashMap<String, String>() {
        {
          put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), "/zone/instance");
          put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), "TRUE");
          put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), "zone");
        }
      };

      admin.setConfig(configScope, clusterProperties);

      for (int i = 0; i < servers; i++) {
        VeniceServerWrapper server = cluster.getVeniceServers().get(i);
        String instanceName = server.getHost() + "_" + server.getPort();
        String domain = "zone=zone_" + (char) (i % replicationFactor + 65) + ",instance=" + instanceName;

        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfig.setDomain(domain);
        instanceConfig.setHostName(server.getHost());
        instanceConfig.setPort(String.valueOf(server.getPort()));

        admin.setInstanceConfig(cluster.getClusterName(), instanceName, instanceConfig);
      }

      String storeName = cluster.createStore(1);

      client.start();

      for (int i = 0; i < servers; i++) {
        HttpGet httpsRequest = new HttpGet(
            "http://" + cluster.getVeniceServers().get(i).getAddress() + "/"
                + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName);
        HttpResponse httpsResponse = client.execute(httpsRequest, null).get();
        Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), 200);

        try (InputStream bodyStream = httpsResponse.getEntity().getContent()) {
          byte[] body = IOUtils.toByteArray(bodyStream);
          Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
          RecordDeserializer<MetadataResponseRecord> metadataResponseRecordRecordDeserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(MetadataResponseRecord.SCHEMA$);
          GenericRecord metadataResponse = metadataResponseRecordRecordDeserializer.deserialize(body);

          try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            GenericDatumWriter<Object> avroDatumWriter = new GenericDatumWriter<>(MetadataResponseRecord.SCHEMA$);
            Encoder jsonEncoder = AvroCompatibilityHelper.newJsonEncoder(MetadataResponseRecord.SCHEMA$, output, true);
            avroDatumWriter.write(metadataResponse, jsonEncoder);
            jsonEncoder.flush();
            output.flush();

            LOGGER.info("Got a metadata response from server {} : {}", i, output);
          } catch (IOException e) {
            throw new VeniceException(e);
          }

          // check we can parse the fields of the response
          Assert.assertEquals(
              ((HashMap<Utf8, Utf8>) metadataResponse.get("keySchema")).get(new Utf8("1")),
              new Utf8("\"int\""));

          // verify the property that no replicas of the same partition are in the same helix group
          Map<Utf8, Integer> helixGroupInfo = (HashMap<Utf8, Integer>) metadataResponse.get("helixGroupInfo");
          Map<Utf8, Collection<Utf8>> routingInfo =
              (HashMap<Utf8, Collection<Utf8>>) metadataResponse.get("routingInfo");

          for (Map.Entry<Utf8, Collection<Utf8>> entry: routingInfo.entrySet()) {
            Set<Integer> zonesSeen = new HashSet<>();
            for (Utf8 instance: entry.getValue()) {
              Assert.assertFalse(
                  zonesSeen.contains(helixGroupInfo.get(instance)),
                  instance + " is in the same helix zone as another replica of the partition");
              zonesSeen.add(helixGroupInfo.get(instance));
            }
          }
        }
      }
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceServerWithD2(boolean https) throws Exception {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      String storeName = cluster.createStore(1);
      Map<String, String> clusterToServerD2 = cluster.getClusterToServerD2();
      String d2ServiceName = clusterToServerD2.get(cluster.getClusterName());

      D2Client d2Client;
      if (https) {
        d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());
      } else {
        d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
      }

      URI requestUri =
          URI.create("d2://" + d2ServiceName + "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName);
      RestRequest request = new RestRequestBuilder(requestUri).setMethod("GET").build();
      RestResponse response = d2Client.restRequest(request).get();

      Assert.assertEquals(response.getStatus(), HttpStatus.SC_OK);
    }
  }
}
