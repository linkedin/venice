package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;


public class R2StorageNodeClient implements StorageNodeClient {
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Map<String, Client> nodeIdToR2ClientMap = new VeniceConcurrentHashMap<>();
  private final List<TransportClientFactory> transportClientFactoryList = Collections.synchronizedList(new ArrayList());
  private final boolean http2Enabled;

  private final int httpMaxResponseSize;

  public R2StorageNodeClient(Optional<SSLEngineComponentFactory> sslFactory, VeniceRouterConfig config) {
    this.sslFactory = sslFactory;
    this.httpMaxResponseSize = config.getRouterHTTPMaxResponseSize();
    this.http2Enabled = config.isRouterHTTP2R2ClientEnabled();
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack,
      long queryStartTimeInNS) throws RouterException {

    RestRequest request = path.composeRestRequest(host.getHostUrl(sslFactory.isPresent()));

    Client selectedClient = nodeIdToR2ClientMap.computeIfAbsent(host.getNodeId(), h -> buildR2Client(sslFactory));
    selectedClient.restRequest(request, new R2ClientCallback(completedCallBack, failedCallBack, cancelledCallBack));
  }

  public Client buildR2Client(Optional<SSLEngineComponentFactory> sslEngineComponentFactory) {
    TransportClientFactory transportClientFactory = new HttpClientFactory.Builder()
        .setUsePipelineV2(http2Enabled)
        .build();
    transportClientFactoryList.add(transportClientFactory);
    final Map<String, Object> properties = new HashMap();
    if (sslEngineComponentFactory.isPresent()) {
      properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslEngineComponentFactory.get().getSSLContext());
      properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslEngineComponentFactory.get().getSSLParameters());
      if (http2Enabled) {
        properties.put(HttpClientFactory.HTTP_PROTOCOL_VERSION, HttpProtocolVersion.HTTP_2.toString());
      }
    }
    properties.put(HttpClientFactory.HTTP_MAX_RESPONSE_SIZE, String.valueOf(httpMaxResponseSize));


    return new TransportClientAdapter(transportClientFactory.getClient(properties));
  }

  @Override
  public void sendRequest(VeniceMetaDataRequest request, CompletableFuture<PortableHttpResponse> responseFuture) {
    String uri = request.getUrl() + request.getQuery();
    URI requestUri;

    try {
      requestUri = new URI(uri);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + uri, e);
    }
    RestRequest restRequest = new RestRequestBuilder(requestUri).setMethod(request.getMethod()).build();

    Client selectedClient = nodeIdToR2ClientMap.computeIfAbsent(request.getNodeId(), h -> buildR2Client(sslFactory));

    if (request.hasTimeout()) {
      RequestContext requestContext = new RequestContext();
      requestContext.getLocalAttrs().put(R2Constants.REQUEST_TIMEOUT, request.getTimeout());
      selectedClient.restRequest(restRequest,
          requestContext,
          new R2ClientCallback(responseFuture::complete, responseFuture::completeExceptionally, () -> responseFuture.cancel(false)));
    } else {
      selectedClient.restRequest(restRequest,
          new R2ClientCallback(responseFuture::complete, responseFuture::completeExceptionally, () -> responseFuture.cancel(false)));
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void close() {
    for (TransportClientFactory factory: transportClientFactoryList) {
      factory.shutdown(null);
    }
    nodeIdToR2ClientMap.forEach((k,v) ->  v.shutdown(null));
  }
}
