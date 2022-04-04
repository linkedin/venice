package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;


public class TransportClientResponseForRoute extends TransportClientResponse {
  private String routeId;

  public TransportClientResponseForRoute(String routeId, int schemaId, CompressionStrategy compressionStrategy,
      byte[] body) {
    super(schemaId, compressionStrategy, body);
    this.routeId = routeId;
  }

  public static TransportClientResponseForRoute fromTransportClientWithRoute(
      TransportClientResponse transportClientResponse, String replicaId) {
    if ( transportClientResponse == null) {
      return new TransportClientResponseForRoute(replicaId, -1,
          null, null);
    } else {
      return new TransportClientResponseForRoute(replicaId, transportClientResponse.getSchemaId(),
          transportClientResponse.getCompressionStrategy(), transportClientResponse.getBody());
    }
  }

  public String getRouteId() {
    return routeId;
  }
}
