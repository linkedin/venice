package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


public interface FastClientTransport {
  CompletableFuture<TransportClientResponse> singleGet(String route, GetRequestContext getRequestContext);

  <K, V> CompletableFuture<TransportClientResponse> multiKeyStreamingRequest(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      Map<String, String> headers);

  void close() throws IOException;
}
