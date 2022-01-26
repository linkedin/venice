package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;


public class TransportClientResponseForRoute extends TransportClientResponse {
  private String routeId;

  public TransportClientResponseForRoute(String routeId, int schemaId, CompressionStrategy compressionStrategy,
      byte[] body) {
    super(schemaId, compressionStrategy, body);
    this.routeId = routeId;
  }

  public static TransportClientResponseForRoute fromTransportClientWithRoute(
      TransportClientResponse transportClientResponse, String replicaId) {
    return new TransportClientResponseForRoute(replicaId, transportClientResponse.getSchemaId(),
        transportClientResponse.getCompressionStrategy(), transportClientResponse.getBody());
  }

  public String getRouteId() {
    return routeId;
  }
}
