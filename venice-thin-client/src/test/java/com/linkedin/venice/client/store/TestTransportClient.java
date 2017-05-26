package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.rest.RestResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class TestTransportClient {
  private static String SERVICE_NAME = "test-service";
  private static String TEST_REQUEST = "test-request";

  private static String D2_PREFIX = "d2://";
  private static String HTTP_PREFIX = "http://";

  private ClientHttpCallback _mockClientHttpCallback;
  private DeserializerFetcher mockDeserializerFetcher;

  private D2TransportClient d2TransportClient;
  private D2Client mockD2Client;

  private HttpTransportClient httpTransportClient;
  private CloseableHttpAsyncClient mockHttpClient;

  @BeforeClass
  public void setUpTransportClient() {
    _mockClientHttpCallback = mock(ClientHttpCallback.class);
    mockDeserializerFetcher = mock(DeserializerFetcher.class);

    mockD2Client = mock(D2Client.class);
    d2TransportClient = new D2TransportClient(SERVICE_NAME, mockD2Client);
    d2TransportClient.setDeserializerFetcher(mockDeserializerFetcher);

    mockHttpClient = mock(CloseableHttpAsyncClient.class);
    httpTransportClient = new HttpTransportClient(HTTP_PREFIX + SERVICE_NAME, mockHttpClient);
    d2TransportClient.setDeserializerFetcher(mockDeserializerFetcher);
  }

  @AfterClass
  public void cleanUp() {
    d2TransportClient.close();
    httpTransportClient.close();
  }

  @Test
  public void testGetByPath() {
    //test D2TransportClient
    d2TransportClient.get(TEST_REQUEST, _mockClientHttpCallback);
    ArgumentCaptor<RestRequest> d2RequestCaptor = ArgumentCaptor.forClass(RestRequest.class);

    verify(mockD2Client).restRequest(d2RequestCaptor.capture(), (Callback<RestResponse>) any());
    Assert.assertEquals(D2_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST,
        d2RequestCaptor.getValue().getURI().toString());

    //test HttpTransportClient
    httpTransportClient.get(TEST_REQUEST, _mockClientHttpCallback);
    ArgumentCaptor<HttpGet> httpRequestCaptor = ArgumentCaptor.forClass(HttpGet.class);

    verify(mockHttpClient).execute(httpRequestCaptor.capture(), any());
    Assert.assertEquals(HTTP_PREFIX + SERVICE_NAME + "/" + TEST_REQUEST,
        httpRequestCaptor.getValue().getURI().toString());
  }
}
