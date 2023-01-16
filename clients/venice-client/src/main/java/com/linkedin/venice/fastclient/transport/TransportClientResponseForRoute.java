package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import java.util.Objects;


public class TransportClientResponseForRoute extends TransportClientResponse {
  private final String routeId;

  public TransportClientResponseForRoute(
      String routeId,
      int schemaId,
      CompressionStrategy compressionStrategy,
      byte[] body) {
    super(schemaId, compressionStrategy, body);
    this.routeId = routeId;
  }

  public static TransportClientResponseForRoute fromTransportClientWithRoute(
      TransportClientResponse transportClientResponse,
      String replicaId) {
    if (transportClientResponse == null) {
      return new TransportClientResponseForRoute(replicaId, -1, null, null);
    } else {
      return new TransportClientResponseForRoute(
          replicaId,
          transportClientResponse.getSchemaId(),
          transportClientResponse.getCompressionStrategy(),
          transportClientResponse.getBody());
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
