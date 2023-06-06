package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.common.callback.Callback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.rest.RestResponseBuilder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.authentication.ClientAuthenticationProvider;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestTransportClient {
  private static String SERVICE_NAME = "test-service";
  private static String UPDATED_SERVICE_NAME = "test-service-2";
  private static String TEST_REQUEST = "storage/test-store";

  private static String D2_PREFIX = "d2://";
  private static String HTTP_PREFIX = "http://";

  private D2TransportClient d2TransportClient;
  private D2Client mockD2Client;

  private HttpTransportClient httpTransportClient;
  private CloseableHttpAsyncClient mockHttpClient;

  @BeforeMethod
  public void setUpTransportClient() {
    mockD2Client = mock(D2Client.class);
    d2TransportClient = new D2TransportClient(SERVICE_NAME, mockD2Client, ClientAuthenticationProvider.DISABLED);

    mockHttpClient = mock(CloseableHttpAsyncClient.class);
    httpTransportClient = new HttpTransportClient(HTTP_PREFIX + SERVICE_NAME, mockHttpClient, null);
  }

  @AfterMethod
  public void cleanUp() {
    d2TransportClient.close();
    httpTransportClient.close();
  }

  @Test
  public void testGetByPath() {
    // test D2TransportClient
    d2TransportClient.get(TEST_REQUEST);
    ArgumentCaptor<RestRequest> d2RequestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    verify(mockD2Client).restRequest(d2RequestCaptor.capture(), any(), (Callback<RestResponse>) any());
    Assert.assertEquals(D2_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST, d2RequestCaptor.getValue().getURI().toString());

    // test HttpTransportClient
    httpTransportClient.get(TEST_REQUEST);
    ArgumentCaptor<HttpGet> httpRequestCaptor = ArgumentCaptor.forClass(HttpGet.class);

    verify(mockHttpClient).execute(httpRequestCaptor.capture(), any());
    Assert.assertEquals(
        HTTP_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST,
        httpRequestCaptor.getValue().getURI().toString());
  }

  @Test
  public void testD2TransportClientHandlesRedirect() {
    RestResponse restResponse = new RestResponseBuilder().setStatus(HttpStatus.SC_MOVED_PERMANENTLY)
        .setHeader(HttpHeaders.LOCATION, D2_PREFIX + UPDATED_SERVICE_NAME + "/" + TEST_REQUEST)
        .build();
    RestException restException = new RestException(restResponse);
    doAnswer(invocation -> {
      Callback<RestResponse> callback = invocation.getArgument(2);
      callback.onError(restException);
      return null;
    }).when(mockD2Client).restRequest(any(), any(), any());

    d2TransportClient.get(TEST_REQUEST);
    ArgumentCaptor<RestRequest> d2RequestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    verify(mockD2Client, times(2)).restRequest(d2RequestCaptor.capture(), any(), any());
    Assert.assertEquals(
        D2_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST,
        d2RequestCaptor.getAllValues().get(0).getURI().toString());
    Assert.assertTrue(
        d2RequestCaptor.getAllValues().get(0).getHeaders().containsKey(HttpConstants.VENICE_ALLOW_REDIRECT));
    Assert.assertEquals(
        D2_PREFIX + UPDATED_SERVICE_NAME + "/" + TEST_REQUEST,
        d2RequestCaptor.getAllValues().get(1).getURI().toString());
    Assert.assertFalse(
        d2RequestCaptor.getAllValues().get(1).getHeaders().containsKey(HttpConstants.VENICE_ALLOW_REDIRECT));
  }
}
