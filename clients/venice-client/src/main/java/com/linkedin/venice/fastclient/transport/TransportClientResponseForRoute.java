package com.linkedin.venice.fastclient.transport;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;


public class TransportClientResponseForRoute extends TransportClientResponse {
  private final String routeId;

  public CompletableFuture<HttpStatus> getRouteRequestFuture() {
    return routeRequestFuture;
  }

  private final CompletableFuture<HttpStatus> routeRequestFuture;

  public TransportClientResponseForRoute(
      String routeId,
      int schemaId,
      CompressionStrategy compressionStrategy,
      byte[] body,
      CompletableFuture<HttpStatus> routeRequestFuture) {
    super(schemaId, compressionStrategy, body);
    this.routeId = routeId;
    this.routeRequestFuture = routeRequestFuture;
  }

  public static TransportClientResponseForRoute fromTransportClientWithRoute(
      TransportClientResponse transportClientResponse,
      String replicaId,
      CompletableFuture<HttpStatus> routeRequestFuture) {
    if (transportClientResponse == null) {
      return new TransportClientResponseForRoute(replicaId, -1, null, null, routeRequestFuture);
    } else {
      return new TransportClientResponseForRoute(
          replicaId,
          transportClientResponse.getSchemaId(),
          transportClientResponse.getCompressionStrategy(),
          transportClientResponse.getBody(),
          routeRequestFuture);
    }
  }

  public String getRouteId() {
    return routeId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    TransportClientResponseForRoute res = (TransportClientResponseForRoute) o;
    return Objects.equals(res.routeId, routeId);
  }

  @Override
  public int hashCode() {
    int res = super.hashCode();
    res = 31 * res + routeId.hashCode();
    return res;
  }
}
