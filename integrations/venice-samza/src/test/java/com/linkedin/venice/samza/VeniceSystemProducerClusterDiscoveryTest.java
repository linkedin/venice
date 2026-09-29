package com.linkedin.venice.samza;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.callback.Callback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Time;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.samza.SamzaException;
import org.testng.annotations.Test;


/**
 * With D2 clients, the producer must find the store's cluster through the routers' cluster discovery D2 service in
 * the child colo, and must not send discovery to the controllers.
 */
public class VeniceSystemProducerClusterDiscoveryTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "venice-7";
  private static final String KME_SYSTEM_STORE = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();

  @Test
  public void testDiscoversTheClusterThroughTheRouters() {
    RouterDiscovery routers = new RouterDiscovery(false);
    D2Client primaryControllerColoD2Client = mock(D2Client.class);
    VeniceSystemProducer producer =
        newProducer(routers.d2Client, primaryControllerColoD2Client, null, false, mock(Time.class));

    producer.setupClientsAndReInitProvider();

    assertEquals(
        routers.requests("d2://" + ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "/discover_cluster/"),
        1,
        "Expected one lookup through the default cluster discovery service, requests: " + routers.uris);
    assertEquals(producer.getControllerClient().getClusterName(), CLUSTER_NAME);
    verifyNoInteractions(primaryControllerColoD2Client);
  }

  @Test
  public void testUsesTheConfiguredClusterDiscoveryService() {
    RouterDiscovery routers = new RouterDiscovery(false);
    VeniceSystemProducer producer =
        newProducer(routers.d2Client, mock(D2Client.class), "venice-discovery-custom", false, mock(Time.class));

    producer.setupClientsAndReInitProvider();

    assertEquals(
        routers.requests("d2://venice-discovery-custom/discover_cluster/" + STORE_NAME),
        1,
        "Requests: " + routers.uris);
    assertEquals(
        routers.requests("d2://" + ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "/"),
        0,
        "Requests: " + routers.uris);
  }

  @Test
  public void testTransportReinitializationDiscoversThroughTheRouters() throws Exception {
    RouterDiscovery routers = new RouterDiscovery(false);
    D2Client primaryControllerColoD2Client = mock(D2Client.class);
    VeniceSystemProducer producer =
        newProducer(routers.d2Client, primaryControllerColoD2Client, null, false, mock(Time.class));
    producer.setupClientsAndReInitProvider();
    String storeDiscovery =
        "d2://" + ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "/discover_cluster/" + STORE_NAME;
    assertEquals(routers.requests(storeDiscovery), 1);

    TransportClient reinitialized = producer.getReinitProvider().apply();

    assertEquals(routers.requests(storeDiscovery), 2, "Requests: " + routers.uris);
    reinitialized.close();
    verifyNoInteractions(primaryControllerColoD2Client);
  }

  /**
   * The Kafka message envelope system store lookup gets the same ten attempts as the store lookup.
   */
  @Test
  public void testKafkaMessageEnvelopeDiscoveryGetsTenAttempts() throws Exception {
    RouterDiscovery routers = new RouterDiscovery(true);
    D2Client primaryControllerColoD2Client = mock(D2Client.class);
    Time time = mock(Time.class);
    VeniceSystemProducer producer = newProducer(routers.d2Client, primaryControllerColoD2Client, null, true, time);

    SamzaException e = expectThrows(SamzaException.class, producer::setupClientsAndReInitProvider);

    assertEquals(
        routers.requests("/discover_cluster/" + KME_SYSTEM_STORE),
        10,
        "Expected ten lookups of the system store, requests: " + routers.uris);
    verify(time, times(10)).sleep(anyLong());
    assertTrue(e.getMessage().contains("Router"), e.getMessage());
    verifyNoInteractions(primaryControllerColoD2Client);
  }

  private static VeniceSystemProducer newProducer(
      D2Client childColoD2Client,
      D2Client primaryControllerColoD2Client,
      String clusterDiscoveryD2ServiceName,
      boolean verifyLatestProtocolPresent,
      Time time) {
    VeniceSystemProducerConfig.Builder builder = new VeniceSystemProducerConfig.Builder().setStoreName(STORE_NAME)
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("push-job-id-1")
        .setRunningFabric("dc-0")
        .setVerifyLatestProtocolPresent(verifyLatestProtocolPresent)
        .setFactory(mock(VeniceSystemFactory.class))
        .setProvidedChildColoD2Client(childColoD2Client)
        .setProvidedPrimaryControllerColoD2Client(primaryControllerColoD2Client)
        .setPrimaryControllerD2ServiceName("ChildController")
        .setTime(time);
    if (clusterDiscoveryD2ServiceName != null) {
      builder.setClusterDiscoveryD2ServiceName(clusterDiscoveryD2ServiceName);
    }
    return new VeniceSystemProducer(builder.build());
  }

  /**
   * A child colo D2 client whose routers answer /discover_cluster for the test store, and optionally report the Kafka
   * message envelope system store as missing. Every other request gets a 404.
   */
  private static class RouterDiscovery {
    final D2Client d2Client = mock(D2Client.class);
    final List<String> uris = new CopyOnWriteArrayList<>();

    @SuppressWarnings("unchecked")
    RouterDiscovery(boolean systemStoreMissing) {
      doAnswer(invocation -> {
        RestRequest request = invocation.getArgument(0);
        Callback<RestResponse> callback = invocation.getArgument(2);
        String uri = request.getURI().toString();
        uris.add(uri);
        if (uri.endsWith("/discover_cluster/" + STORE_NAME)) {
          callback.onSuccess(discoveryResponse(STORE_NAME));
        } else if (uri.endsWith("/discover_cluster/" + KME_SYSTEM_STORE) && !systemStoreMissing) {
          callback.onSuccess(discoveryResponse(KME_SYSTEM_STORE));
        } else {
          callback.onSuccess(new RestResponseBuilder().setStatus(404).build());
        }
        return null;
      }).when(d2Client).restRequest(any(RestRequest.class), any(RequestContext.class), any(Callback.class));
    }

    long requests(String uriFragment) {
      return uris.stream().filter(uri -> uri.contains(uriFragment)).count();
    }

    private static RestResponse discoveryResponse(String storeName) throws Exception {
      D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
      response.setName(storeName);
      response.setCluster(CLUSTER_NAME);
      response.setD2Service(CLUSTER_NAME);
      return new RestResponseBuilder().setStatus(200)
          .setEntity(ObjectMapperFactory.getInstance().writeValueAsBytes(response))
          .build();
    }
  }
}
