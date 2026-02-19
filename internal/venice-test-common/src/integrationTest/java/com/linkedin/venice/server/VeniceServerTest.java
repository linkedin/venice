package com.linkedin.venice.server;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DELETE_UNASSIGNED_PARTITIONS_ON_STARTUP;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_IS_AUTO_JOIN;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.protocols.VeniceMetadataRequest;
import com.linkedin.venice.protocols.VeniceMetadataResponse;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.constants.InstanceConstants;
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
  static final long TOTAL_TIMEOUT_FOR_LONG_TEST_MS = 70 * Time.MS_PER_SECOND;
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerTest.class);

  @Test
  public void testStartServerWithDefaultConfigForTests() throws NoSuchFieldException, IllegalAccessException {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      TestVeniceServer server = cluster.getVeniceServers().get(0).getVeniceServer();
      Assert.assertTrue(server.isStarted());

      Field liveClusterConfigRepoField = server.getClass().getSuperclass().getDeclaredField("liveClusterConfigRepo");
      liveClusterConfigRepoField.setAccessible(true);
      Assert.assertNotNull(liveClusterConfigRepoField.get(server));
    }
  }

  @Test
  public void testStartServerWhenEnableAllowlistCheckingFailed() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(0).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
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
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(0).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
      featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
      cluster.addVeniceServer(featureProperties, new Properties());
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    }
  }

  @Test
  public void testCheckBeforeJoinCluster() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      StorageEngineRepository repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();

      // Create a storage engine.
      String storeName = cluster.createStore(10);
      String storeVersionName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertNotNull(
          repository.getLocalStorageEngine(storeVersionName),
          "Storage engine should be created for: " + storeVersionName);

      // Restart server, as server's info leave in Helix cluster, so we expect that all local storage would NOT be
      // deleted
      // once the server join again.
      cluster.stopVeniceServer(server.getPort());
      cluster.restartVeniceServer(server.getPort());
      repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert
          .assertNotEquals(repository.getAllLocalStorageEngines().size(), 0, "We should not cleanup the local storage");
      Assert.assertNotNull(
          repository.getLocalStorageEngine(storeVersionName),
          "Storage engine should be created for: " + storeVersionName);

      // Stop server, remove it from the cluster then restart. We expect that all local storage would be deleted. Once
      // the server join again.
      cluster.stopVeniceServer(server.getPort());

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> Assert.assertFalse(server.isRunning()));

      cluster.useControllerClient(controllerClient -> {
        controllerClient.removeNodeFromCluster(Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort()));

        TestUtils.waitForNonDeterministicAssertion(
            10,
            TimeUnit.SECONDS,
            () -> Assert.assertEquals(controllerClient.listStorageNodes().getNodes().length, 0));
      });

      cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixAdminClient()
          .manuallyEnableMaintenanceMode(cluster.getClusterName(), true, "Prevent nodes from re-joining", null);

      cluster.restartVeniceServer(server.getPort());
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> Assert.assertTrue(server.isRunning()));

      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert.assertNull(
              server.getVeniceServer()
                  .getStorageService()
                  .getStorageEngineRepository()
                  .getLocalStorageEngine(storeVersionName),
              "After removing the node from cluster, local storage should be cleaned up once the server join the cluster again."));
    }
  }

  @Test
  public void testCheckBeforeJointClusterBeforeHelixInitializingCluster() throws Exception {
    Thread serverAddingThread = null;
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(0).numberOfServers(0).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
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
  public void testStartServerAndShutdownWithPartitionAssignmentVerification() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(0).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
      featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
      featureProperties.setProperty(SERVER_DELETE_UNASSIGNED_PARTITIONS_ON_STARTUP, Boolean.toString(true));
      cluster.addVeniceServer(featureProperties, new Properties());
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      Assert.assertTrue(server.getVeniceServer().isStarted());
      StorageService storageService = server.getVeniceServer().getStorageService();
      StorageEngineRepository repository = storageService.getStorageEngineRepository();

      // Create a storage engine.
      String storeName = Version.composeKafkaTopic(cluster.createStore(1), 1);
      Assert.assertNotNull(
          repository.getLocalStorageEngine(storeName),
          "Storage engine should be created for: " + storeName);
      Assert.assertTrue(server.getVeniceServer().getHelixParticipationService().isRunning());
      Assert.assertEquals(storageService.getStorageEngine(storeName).getPartitionIds().size(), 3);

      cluster.stopVeniceServer(server.getPort());
      Assert.assertFalse(server.getVeniceServer().isStarted());

      // Create new servers so partition assignment is removed for the offline participant
      cluster.addVeniceServer(featureProperties, new Properties());
      cluster.addVeniceServer(featureProperties, new Properties());

      cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixAdminClient()
          .manuallyEnableMaintenanceMode(cluster.getClusterName(), true, "Prevent nodes from re-joining", null);
      cluster.restartVeniceServer(server.getPort());
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(storageService.getStorageEngine(storeName).getPartitionIds().size(), 0));
    }
  }

  @Test
  public void testMetadataFetchRequest() throws ExecutionException, InterruptedException, IOException {
    Utils.thisIsLocalhost();
    int servers = 6;
    int replicationFactor = 2;

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(servers)
        .numberOfRouters(0)
        .replicationFactor(replicationFactor)
        .build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options);
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
      cluster.useControllerClient(
          controllerClient -> Assert.assertFalse(
              controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true))
                  .isError()));
      ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

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
        httpsRequest = new HttpGet(
            "http://" + cluster.getVeniceServers().get(i).getAddress() + "/"
                + QueryAction.CURRENT_VERSION.toString().toLowerCase() + "/" + storeName);
        httpsResponse = client.execute(httpsRequest, null).get();
        Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), 200);
        try (InputStream bodyStream = httpsResponse.getEntity().getContent()) {
          byte[] body = IOUtils.toByteArray(bodyStream);
          Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
          ServerCurrentVersionResponse currentVersionResponse =
              OBJECT_MAPPER.readValue(body, ServerCurrentVersionResponse.class);
          Assert.assertEquals(currentVersionResponse.getCurrentVersion(), 1);
        } catch (IOException e) {
          throw new VeniceException(e);
        }
      }
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceServerWithD2(boolean https) throws Exception {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
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
      Assert.assertThrows(ExecutionException.class, () -> d2Client.restRequest(request).get());
      cluster.useControllerClient(
          controllerClient -> controllerClient
              .updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true)));
      RestResponse response = d2Client.restRequest(request).get();
      Assert.assertEquals(response.getStatus(), HttpStatus.SC_OK);
    }
  }

  /**
   * Verifies feature parity between the HTTP {@code /metadata/{storeName}} endpoint and the gRPC
   * {@code getMetadata} RPC by sending both requests to each server and comparing the raw Avro response bytes.
   */
  @Test
  public void testGrpcMetadataFetchRequest() throws ExecutionException, InterruptedException, IOException {
    Utils.thisIsLocalhost();
    int servers = 3;
    int replicationFactor = 2;

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(servers)
        .numberOfRouters(0)
        .replicationFactor(replicationFactor)
        .enableGrpc(true)
        .build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options);
        CloseableHttpAsyncClient httpClient =
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
      cluster.useControllerClient(
          controllerClient -> Assert.assertFalse(
              controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true))
                  .isError()));

      httpClient.start();

      List<ManagedChannel> channels = new ArrayList<>();
      try {
        for (int i = 0; i < servers; i++) {
          VeniceServerWrapper server = cluster.getVeniceServers().get(i);

          // --- HTTP request ---
          HttpGet httpRequest = new HttpGet(
              "http://" + server.getAddress() + "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName);
          HttpResponse httpResponse = httpClient.execute(httpRequest, null).get();
          Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpStatus.SC_OK);

          byte[] httpBody;
          try (InputStream bodyStream = httpResponse.getEntity().getContent()) {
            httpBody = IOUtils.toByteArray(bodyStream);
          }

          // --- gRPC request ---
          ManagedChannel channel =
              Grpc.newChannelBuilder(server.getGrpcAddress(), InsecureChannelCredentials.create()).build();
          channels.add(channel);

          VeniceReadServiceGrpc.VeniceReadServiceBlockingStub blockingStub =
              VeniceReadServiceGrpc.newBlockingStub(channel);
          VeniceMetadataResponse grpcResponse =
              blockingStub.getMetadata(VeniceMetadataRequest.newBuilder().setStoreName(storeName).build());

          Assert.assertEquals(
              grpcResponse.getErrorCode(),
              VeniceReadResponseStatus.OK,
              "gRPC metadata request failed for server " + i + ": " + grpcResponse.getErrorMessage());

          byte[] grpcBody = grpcResponse.getMetadata().toByteArray();

          // --- Verify HTTP and gRPC return identical Avro bytes ---
          Assert.assertEquals(grpcBody, httpBody, "HTTP and gRPC metadata responses differ for server " + i);

          // Deserialize and verify content
          RecordDeserializer<MetadataResponseRecord> deserializer =
              SerializerDeserializerFactory.getAvroGenericDeserializer(MetadataResponseRecord.SCHEMA$);
          GenericRecord metadataResponse = deserializer.deserialize(grpcBody);

          Assert.assertEquals(
              ((HashMap<Utf8, Utf8>) metadataResponse.get("keySchema")).get(new Utf8("1")),
              new Utf8("\"int\""));

          // Verify the property that no replicas of the same partition are in the same helix group
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
      } finally {
        for (ManagedChannel channel: channels) {
          channel.shutdownNow();
        }
      }
    }
  }

  /**
   * Verifies that the gRPC {@code getMetadata} RPC returns the correct error when the store has read quota disabled.
   */
  @Test
  public void testGrpcMetadataFetchRequestWithQuotaDisabled() {
    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(0)
        .enableGrpc(true)
        .build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      String storeName = cluster.createStore(1);
      // StorageNodeReadQuotaEnabled is false by default, so the metadata endpoint should return an error

      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      ManagedChannel channel =
          Grpc.newChannelBuilder(server.getGrpcAddress(), InsecureChannelCredentials.create()).build();
      try {
        VeniceReadServiceGrpc.VeniceReadServiceBlockingStub blockingStub =
            VeniceReadServiceGrpc.newBlockingStub(channel);

        VeniceMetadataRequest grpcRequest = VeniceMetadataRequest.newBuilder().setStoreName(storeName).build();
        VeniceMetadataResponse grpcResponse = blockingStub.getMetadata(grpcRequest);

        Assert.assertEquals(grpcResponse.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
        Assert.assertFalse(grpcResponse.getErrorMessage().isEmpty(), "Error message should not be empty");
      } finally {
        channel.shutdownNow();
      }
    }
  }

  @Test
  public void testStartServerWithSystemSchemaInitialization() {
    Properties controllerProperties = new Properties();
    // Disable controller meta system store initialization so that server spun up after controller will register schemas
    controllerProperties.setProperty(CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, "false");
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().extraProperties(controllerProperties).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      cluster.useControllerClient(controllerClient -> {
        String storeName = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName();
        int currentProtocolVersion = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore());
        MultiSchemaResponse schemaResponse = controllerClient.getAllValueAndDerivedSchema(storeName);
        Assert.assertNotNull(schemaResponse.getSchemas());
        Assert.assertEquals(schemaResponse.getSchemas().length, currentProtocolVersion * 2);
      });
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testDropStorePartitionAsynchronously() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      Assert.assertTrue(server.getVeniceServer().isStarted());

      StorageService storageService = server.getVeniceServer().getStorageService();
      Assert.assertTrue(server.getVeniceServer().isStarted());
      final StorageEngineRepository repository = storageService.getStorageEngineRepository();
      // Create a new store
      String storeName = cluster.createStore(1);
      String storeVersionName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertNotNull(
          storageService.getStorageEngine(storeVersionName),
          "Storage engine should be created for: " + storeVersionName);
      Assert.assertTrue(server.getVeniceServer().getHelixParticipationService().isRunning());
      Assert.assertEquals(storageService.getStorageEngine(storeVersionName).getPartitionIds().size(), 3);
      String helixInstanceName = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());
      String instanceOperationReason = "Disable instance to remove all partitions assigned to it";
      cluster.getLeaderVeniceController()
          .getVeniceHelixAdmin()
          .getHelixAdminClient()
          .setInstanceOperation(
              cluster.getClusterName(),
              helixInstanceName,
              InstanceConstants.InstanceOperation.DISABLE,
              instanceOperationReason);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // All partitions should have been dropped asynchronously due to instance being disabled
        Assert.assertNull(
            storageService.getStorageEngine(storeVersionName),
            "Storage engine: " + storeVersionName + " should have been dropped");
        Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 0);
      });
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testDropStorePartitionSynchronously() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(0).build();
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
      featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      Assert.assertTrue(server.getVeniceServer().isStarted());

      StorageService storageService = server.getVeniceServer().getStorageService();
      Assert.assertTrue(server.getVeniceServer().isStarted());

      // Create a new store
      String storeName = cluster.createStore(1);
      String storeVersionName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertNotNull(
          storageService.getStorageEngine(storeVersionName),
          "Storage engine should be created for: " + storeVersionName);
      Assert.assertTrue(server.getVeniceServer().getHelixParticipationService().isRunning());
      Assert.assertEquals(storageService.getStorageEngine(storeVersionName).getPartitionIds().size(), 3);

      cluster.useControllerClient(controllerClient -> {
        controllerClient.disableAndDeleteStore(storeName);
      });

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // All partitions should have been dropped synchronously
        Assert.assertNull(
            storageService.getStorageEngine(storeVersionName),
            "Storage engine: " + storeVersionName + " should have been dropped");
      });
    }
  }
}
