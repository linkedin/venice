package com.linkedin.venice.controllerapi;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class D2ControllerClientFactoryTest {
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String STORE_NAME = "test_store";
  private static final String NON_EXISTENT_STORE_NAME = "test_missing_store";
  private static final String NON_EXISTENT_STORE_NAME_LEGACY_RESPONSE = "test_missing_store_legacy_response";

  private static final String CONTROLLER_D2_SERVICE = "controller_d2_service";

  @Test
  public void testSharedControllerClient() throws JsonProcessingException {
    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setName(STORE_NAME);
    discoveryResponse.setCluster(CLUSTER_NAME);
    RestResponse discoverClusterRestResponse = getClusterDiscoveryRestResponse(discoveryResponse);

    D2ServiceDiscoveryResponse nonExistentDiscoveryResponse = new D2ServiceDiscoveryResponse();
    nonExistentDiscoveryResponse.setErrorType(ErrorType.STORE_NOT_FOUND);
    nonExistentDiscoveryResponse.setError("Store " + NON_EXISTENT_STORE_NAME + " not found");
    nonExistentDiscoveryResponse.setName(NON_EXISTENT_STORE_NAME);
    RestResponse nonExistentDiscoverClusterResponse = getClusterDiscoveryRestResponse(nonExistentDiscoveryResponse);

    D2ServiceDiscoveryResponse nonExistentDiscoveryResponseLegacy = new D2ServiceDiscoveryResponse();
    nonExistentDiscoveryResponseLegacy.setError("Store " + NON_EXISTENT_STORE_NAME_LEGACY_RESPONSE + " not found");
    nonExistentDiscoveryResponseLegacy.setName(NON_EXISTENT_STORE_NAME_LEGACY_RESPONSE);
    RestResponse nonExistentDiscoverClusterLegacyResponse =
        getClusterDiscoveryRestResponse(nonExistentDiscoveryResponseLegacy);

    D2Client mockD2Client = Mockito.mock(D2Client.class);
    Mockito.doAnswer(invocation -> {
      RestRequest request = invocation.getArgument(0, RestRequest.class);
      URI uri = request.getURI();
      if (uri.getPath().equals(ControllerRoute.CLUSTER_DISCOVERY.getPath())) {
        CompletableFuture<RestResponse> responseFuture = new CompletableFuture<>();

        Map<String, String> queryParams = Arrays.stream(uri.getQuery().split("&"))
            .map(str -> str.trim().split("="))
            .collect(Collectors.toMap(strArr -> strArr[0], strArr -> strArr[1]));
        String storeName = queryParams.get(ControllerApiConstants.NAME);

        if (storeName.equals(STORE_NAME)) {
          responseFuture.complete(discoverClusterRestResponse);
        } else if (storeName.equals(NON_EXISTENT_STORE_NAME)) {
          responseFuture.completeExceptionally(new RestException(nonExistentDiscoverClusterResponse));
        } else if (storeName.equals(NON_EXISTENT_STORE_NAME_LEGACY_RESPONSE)) {
          responseFuture.completeExceptionally(new RestException(nonExistentDiscoverClusterLegacyResponse));
        }

        if (responseFuture.isDone()) {
          return responseFuture;
        } else {
          return null;
        }
      }
      return null;
    }).when(mockD2Client).restRequest(Mockito.any());

    try (D2ControllerClient client = D2ControllerClientFactory
        .discoverAndConstructControllerClient(STORE_NAME, CONTROLLER_D2_SERVICE, 1, mockD2Client)) {
      Assert.assertEquals(client.getClusterName(), CLUSTER_NAME);
    }

    Assert.assertThrows(
        VeniceNoStoreException.class,
        () -> D2ControllerClientFactory
            .discoverAndConstructControllerClient(NON_EXISTENT_STORE_NAME, CONTROLLER_D2_SERVICE, 1, mockD2Client));

    Assert.assertThrows(
        VeniceException.class,
        () -> D2ControllerClientFactory.discoverAndConstructControllerClient(
            NON_EXISTENT_STORE_NAME_LEGACY_RESPONSE,
            CONTROLLER_D2_SERVICE,
            1,
            mockD2Client));
  }

  private RestResponse getClusterDiscoveryRestResponse(D2ServiceDiscoveryResponse discoveryResponse)
      throws JsonProcessingException {
    String discoverClusterResponse = ObjectMapperFactory.getInstance().writeValueAsString(discoveryResponse);

    RestResponse discoverClusterRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(discoverClusterResponse.getBytes(StandardCharsets.UTF_8)))
        .when(discoverClusterRestResponse)
        .getEntity();

    return discoverClusterRestResponse;
  }
}
