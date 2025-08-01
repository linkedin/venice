package com.linkedin.venice.controllerapi;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestD2ControllerClient {
  private static final String TEST_STORE = "test_store";
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_ZK_ADDRESS = "localhost:2181";
  private static final String TEST_ZK_ADDRESS_2 = "localhost:2182";
  private static final String TEST_ZK_ADDRESS_3 = "localhost:2183";
  private static final List<String> TEST_ZK_ADDRESSES =
      Arrays.asList(TEST_ZK_ADDRESS, TEST_ZK_ADDRESS_2, TEST_ZK_ADDRESS_3);
  private static final String TEST_CONTROLLER_D2_SERVICE = "ChildController";
  private static final String TEST_ROUTER_D2_SERVICE = "VeniceRouter";
  private static final String TEST_SERVER_D2_SERVICE = "VeniceServer";

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    D2ClientFactory.setUnitTestMode();
  }

  @AfterClass(alwaysRun = true)
  public void teardown() {
    D2ClientFactory.resetUnitTestMode();
  }

  @Test
  public void testConstructD2ControllerClient() {
    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, TEST_ZK_ADDRESS, Optional.empty())) {
      // Do nothing since we only want to test the constructor
    }
  }

  @Test
  public void testDiscoverCluster() throws JsonProcessingException {
    D2ServiceDiscoveryResponse serviceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    serviceDiscoveryResponse.setCluster(TEST_CLUSTER);
    serviceDiscoveryResponse.setD2Service(TEST_ROUTER_D2_SERVICE);
    serviceDiscoveryResponse.setServerD2Service(TEST_SERVER_D2_SERVICE);
    serviceDiscoveryResponse.setName(TEST_STORE);

    String discoverClusterResponse = ObjectMapperFactory.getInstance().writeValueAsString(serviceDiscoveryResponse);

    RestResponse discoverClusterRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(discoverClusterResponse.getBytes(StandardCharsets.UTF_8)))
        .when(discoverClusterRestResponse)
        .getEntity();

    D2Client mockD2Client = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(() -> discoverClusterRestResponse);
      }
      return null;
    }).when(mockD2Client).restRequest(any());

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client);

    D2ServiceDiscoveryResponse response = D2ControllerClient
        .discoverCluster(TEST_ZK_ADDRESS, TEST_CONTROLLER_D2_SERVICE, TEST_STORE, 1, Optional.empty());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, mockD2Client)) {
      D2ServiceDiscoveryResponse response1 = controllerClient.discoverCluster(TEST_STORE);
      Assert.assertEquals(response1.getCluster(), TEST_CLUSTER);
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
  }

  @Test
  public void testDiscoverClusterWithMultipleD2Clients() throws JsonProcessingException {
    D2ServiceDiscoveryResponse serviceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    serviceDiscoveryResponse.setCluster(TEST_CLUSTER);
    serviceDiscoveryResponse.setD2Service(TEST_ROUTER_D2_SERVICE);
    serviceDiscoveryResponse.setServerD2Service(TEST_SERVER_D2_SERVICE);
    serviceDiscoveryResponse.setName(TEST_STORE);

    String discoverClusterResponse = ObjectMapperFactory.getInstance().writeValueAsString(serviceDiscoveryResponse);

    RestResponse discoverClusterRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(discoverClusterResponse.getBytes(StandardCharsets.UTF_8)))
        .when(discoverClusterRestResponse)
        .getEntity();

    D2Client mockD2Client1 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client1).restRequest(any());

    D2Client mockD2Client2 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(() -> discoverClusterRestResponse);
      }
      return null;
    }).when(mockD2Client2).restRequest(any());

    D2Client mockD2Client3 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client3).restRequest(any());

    List<D2Client> d2Clients = Arrays.asList(mockD2Client1, mockD2Client2, mockD2Client3);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, d2Clients, Optional.empty())) {
      try {
        D2ServiceDiscoveryResponse response = controllerClient.discoverCluster(TEST_STORE);
        Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client1);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_2, mockD2Client2);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_3, mockD2Client3);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, Optional.empty(), TEST_ZK_ADDRESSES)) {
      try {
        D2ServiceDiscoveryResponse response = controllerClient.discoverCluster(TEST_STORE);
        Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
    D2ClientFactory.release(TEST_ZK_ADDRESS_2);
    D2ClientFactory.release(TEST_ZK_ADDRESS_3);
  }

  @Test
  public void testDiscoverClusterFailure() throws JsonProcessingException {
    D2ServiceDiscoveryResponse serviceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    serviceDiscoveryResponse.setError(new VeniceException("FAIL"));
    serviceDiscoveryResponse.setName(TEST_STORE);

    String discoverClusterResponse = ObjectMapperFactory.getInstance().writeValueAsString(serviceDiscoveryResponse);

    RestResponse discoverClusterRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(discoverClusterResponse.getBytes(StandardCharsets.UTF_8)))
        .when(discoverClusterRestResponse)
        .getEntity();

    D2Client mockD2Client = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(() -> discoverClusterRestResponse);
      }
      return null;
    }).when(mockD2Client).restRequest(any());

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client);
    Assert.assertTrue(
        D2ControllerClient.discoverCluster(TEST_ZK_ADDRESS, TEST_CONTROLLER_D2_SERVICE, TEST_STORE, 1, Optional.empty())
            .isError());

    Assert.assertTrue(
        D2ControllerClient.discoverCluster(mockD2Client, TEST_CONTROLLER_D2_SERVICE, TEST_STORE, 1).isError());

    try (D2ControllerClient controllerClient =
        D2ControllerClientFactory.getControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, mockD2Client)) {
      // D2ControllerClient should follow the behavior of the base class and return an error response
      D2ServiceDiscoveryResponse response = controllerClient.discoverCluster(TEST_STORE);
      Assert.assertTrue(response.isError());
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
  }

  @Test
  public void testDiscoverClusterThrowsExceptionWithMultipleD2Clients() {
    D2Client mockD2Client1 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client1).restRequest(any());

    D2Client mockD2Client2 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client2).restRequest(any());

    List<D2Client> d2Clients = Arrays.asList(mockD2Client1, mockD2Client2);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, d2Clients, Optional.empty())) {
      try {
        controllerClient.discoverCluster(TEST_STORE);
      } catch (VeniceException e) {
        Assert.assertEquals(e.getMessage(), "Failed to discover cluster with D2 client");
      }
      Assert.assertThrows(VeniceException.class, () -> controllerClient.discoverCluster(TEST_STORE));
    }

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client1);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_2, mockD2Client2);

    List<String> testZkAddresses = Arrays.asList(TEST_ZK_ADDRESS, TEST_ZK_ADDRESS_2);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, Optional.empty(), testZkAddresses)) {
      try {
        controllerClient.discoverCluster(TEST_STORE);
      } catch (VeniceException e) {
        Assert.assertEquals(e.getMessage(), "Failed to discover cluster with D2 client");
      }
      Assert.assertThrows(VeniceException.class, () -> controllerClient.discoverCluster(TEST_STORE));
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
    D2ClientFactory.release(TEST_ZK_ADDRESS_2);
  }

  @Test
  public void testDiscoverLeaderController() throws JsonProcessingException {
    LeaderControllerResponse leaderControllerResponse = new LeaderControllerResponse();
    leaderControllerResponse.setCluster(TEST_CLUSTER);
    leaderControllerResponse.setName(TEST_STORE);
    leaderControllerResponse.setUrl("http://localhost:2000");
    leaderControllerResponse.setSecureUrl("http://localhost:2001");

    String leaderControllerResponseStr = ObjectMapperFactory.getInstance().writeValueAsString(leaderControllerResponse);

    RestResponse leaderControllerRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(leaderControllerResponseStr.getBytes(StandardCharsets.UTF_8)))
        .when(leaderControllerRestResponse)
        .getEntity();

    D2Client mockD2Client = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(() -> leaderControllerRestResponse);
      }
      return null;
    }).when(mockD2Client).restRequest(any());

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, mockD2Client, Optional.empty())) {
      String leaderController = controllerClient.discoverLeaderController();
      Assert.assertEquals(leaderController, leaderControllerResponse.getUrl());
    }

    try (D2ControllerClient controllerClient = new D2ControllerClient(
        TEST_CONTROLLER_D2_SERVICE,
        TEST_CLUSTER,
        mockD2Client,
        Optional.of(mock(SSLFactory.class)))) {
      String leaderController = controllerClient.discoverLeaderController();
      Assert.assertEquals(leaderController, leaderControllerResponse.getSecureUrl());
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
  }

  @Test
  public void testDiscoverLeaderControllerWithLegacySslPort() throws JsonProcessingException, MalformedURLException {
    LeaderControllerResponse leaderControllerResponse = new LeaderControllerResponse();
    leaderControllerResponse.setCluster(TEST_CLUSTER);
    leaderControllerResponse.setName(TEST_STORE);
    leaderControllerResponse.setUrl("http://localhost:2000");

    String leaderControllerResponseStr = ObjectMapperFactory.getInstance().writeValueAsString(leaderControllerResponse);

    RestResponse leaderControllerRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(leaderControllerResponseStr.getBytes(StandardCharsets.UTF_8)))
        .when(leaderControllerRestResponse)
        .getEntity();

    D2Client mockD2Client = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(() -> leaderControllerRestResponse);
      }
      return null;
    }).when(mockD2Client).restRequest(any());

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client);

    try (D2ControllerClient controllerClient = new D2ControllerClient(
        TEST_CONTROLLER_D2_SERVICE,
        TEST_CLUSTER,
        mockD2Client,
        Optional.of(mock(SSLFactory.class)))) {
      URL responseUrl = new URL(controllerClient.discoverLeaderController());
      Assert.assertEquals(responseUrl.getPort(), 1578);
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
  }

  @Test
  public void testDiscoverLeaderControllerWithMultipleD2Clients() throws JsonProcessingException {
    LeaderControllerResponse leaderControllerResponse = new LeaderControllerResponse();
    leaderControllerResponse.setCluster(TEST_CLUSTER);
    leaderControllerResponse.setName(TEST_STORE);
    leaderControllerResponse.setUrl("http://localhost:2000");
    leaderControllerResponse.setSecureUrl("http://localhost:2001");

    String leaderControllerResponseStr = ObjectMapperFactory.getInstance().writeValueAsString(leaderControllerResponse);

    RestResponse leaderControllerRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(leaderControllerResponseStr.getBytes(StandardCharsets.UTF_8)))
        .when(leaderControllerRestResponse)
        .getEntity();

    D2Client mockD2Client1 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client1).restRequest(any());

    D2Client mockD2Client2 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(() -> leaderControllerRestResponse);
      }
      return null;
    }).when(mockD2Client2).restRequest(any());

    D2Client mockD2Client3 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client3).restRequest(any());

    List<D2Client> d2Clients = Arrays.asList(mockD2Client1, mockD2Client2, mockD2Client3);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, d2Clients, Optional.empty())) {
      try {
        String leaderController = controllerClient.discoverLeaderController();
        Assert.assertEquals(leaderController, leaderControllerResponse.getUrl());
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    try (D2ControllerClient controllerClient = new D2ControllerClient(
        TEST_CONTROLLER_D2_SERVICE,
        TEST_CLUSTER,
        d2Clients,
        Optional.of(mock(SSLFactory.class)))) {
      try {
        String leaderController = controllerClient.discoverLeaderController();
        Assert.assertEquals(leaderController, leaderControllerResponse.getSecureUrl());
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client1);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_2, mockD2Client2);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_3, mockD2Client3);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, Optional.empty(), TEST_ZK_ADDRESSES)) {
      try {
        String leaderController = controllerClient.discoverLeaderController();
        Assert.assertEquals(leaderController, leaderControllerResponse.getUrl());
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    try (D2ControllerClient controllerClient = new D2ControllerClient(
        TEST_CONTROLLER_D2_SERVICE,
        TEST_CLUSTER,
        Optional.of(mock(SSLFactory.class)),
        TEST_ZK_ADDRESSES)) {
      try {
        String leaderController = controllerClient.discoverLeaderController();
        Assert.assertEquals(leaderController, leaderControllerResponse.getSecureUrl());
      } catch (VeniceException e) {
        Assert.fail("The method call should return a response and not throw an exception");
      }
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
    D2ClientFactory.release(TEST_ZK_ADDRESS_2);
    D2ClientFactory.release(TEST_ZK_ADDRESS_3);
  }

  @Test
  public void testDiscoverLeaderControllerThrowsExceptionWithMultipleD2Clients() {
    D2Client mockD2Client1 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client1).restRequest(any());

    D2Client mockD2Client2 = Mockito.mock(D2Client.class);
    doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.LEADER_CONTROLLER.getPath())) {
        return CompletableFuture.supplyAsync(VeniceException::new);
      }
      return null;
    }).when(mockD2Client2).restRequest(any());

    List<D2Client> d2Clients = Arrays.asList(mockD2Client1, mockD2Client2);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, d2Clients, Optional.empty())) {
      try {
        controllerClient.discoverLeaderController();
      } catch (VeniceException e) {
        Assert.assertEquals(e.getMessage(), "Failed to discover leader controller with D2 client");
      }
      Assert.assertThrows(VeniceException.class, () -> controllerClient.discoverLeaderController());
    }

    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS, mockD2Client1);
    D2ClientFactory.setD2Client(TEST_ZK_ADDRESS_2, mockD2Client2);

    List<String> testZkAddresses = Arrays.asList(TEST_ZK_ADDRESS, TEST_ZK_ADDRESS_2);

    try (D2ControllerClient controllerClient =
        new D2ControllerClient(TEST_CONTROLLER_D2_SERVICE, TEST_CLUSTER, Optional.empty(), testZkAddresses)) {
      try {
        controllerClient.discoverLeaderController();
      } catch (VeniceException e) {
        Assert.assertEquals(e.getMessage(), "Failed to discover leader controller with D2 client");
      }
      Assert.assertThrows(VeniceException.class, () -> controllerClient.discoverLeaderController());
    }

    D2ClientFactory.release(TEST_ZK_ADDRESS);
    D2ClientFactory.release(TEST_ZK_ADDRESS_2);
  }

  /**
   * TODO: Remove the below unit test after controller ACL migration
   */
  @Test
  public void testHelperFunctionToConvertUrl() throws MalformedURLException {
    URL testUrl = new URL("http://localhost:1576");
    Assert.assertEquals(D2ControllerClient.convertToSecureUrl(testUrl).toString(), "https://localhost:1578");
    Assert.assertEquals(D2ControllerClient.convertToSecureUrl(testUrl, 2000).toString(), "https://localhost:2000");
  }
}
