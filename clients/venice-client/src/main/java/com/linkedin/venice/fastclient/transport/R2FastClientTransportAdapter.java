package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import com.linkedin.venice.utils.LatencyUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


/**
 * Adapter class to adapt {@link TransportClient} impl to {@link FastClientTransport}.
 */
public class R2FastClientTransportAdapter implements FastClientTransport {
  private TransportClient transportClient;

  public R2FastClientTransportAdapter(TransportClient transportClient) {
    this.transportClient = transportClient;
  }

  @Override
  public CompletableFuture<TransportClientResponse> singleGet(String route, GetRequestContext getRequestContext) {
    return transportClient.get(route + getRequestContext.computeRequestUri());
  }

  @Override
  public <K, V> CompletableFuture<TransportClientResponse> multiKeyStreamingRequest(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      Map<String, String> requestHeaders) {
    String url = route + requestContext.computeRequestUri();
    long nanoTsBeforeSerialization = System.nanoTime();
    byte[] serializedRequest = requestSerializer.apply(keysForRoutes);
    requestContext.recordRequestSerializationTime(route, LatencyUtils.getLatencyInNS(nanoTsBeforeSerialization));
    return transportClient.post(url, requestHeaders, serializedRequest);
  }

  @Override
  public void close() throws IOException {
    transportClient.close();
  }
}
