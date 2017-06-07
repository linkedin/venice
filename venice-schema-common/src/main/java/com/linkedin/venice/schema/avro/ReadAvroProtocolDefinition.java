package com.linkedin.venice.schema.avro;

import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.util.Optional;
import org.apache.avro.specific.SpecificRecord;

public enum ReadAvroProtocolDefinition {
  /**
   * Client request for single-get v1.
   */
  SINGLE_GET_CLIENT_REQUEST_V1(1, Optional.empty()),

  /**
   * Client request for multi-get v1.
   */
  MULTI_GET_CLIENT_REQUEST_V1(1, Optional.empty()),

  /**
   * Router request key for multi-get v1.
   */
  MULTI_GET_ROUTER_REQUEST_V1(1, Optional.of(MultiGetRouterRequestKeyV1.class)),
  /**
   * Response record for multi-get v1.
   */
  MULTI_GET_RESPONSE_V1(1, Optional.of(MultiGetResponseRecordV1.class));

  /**
   * Current version being used.
   */
  final int protocolVersion;

  /**
   * The {@link SpecificRecord} class being used currently.
   */
  final Optional<Class<? extends SpecificRecord>> specificRecordClass;

  ReadAvroProtocolDefinition(int protocolVersion, Optional<Class<? extends SpecificRecord>> specificRecordClass) {
    this.protocolVersion = protocolVersion;
    this.specificRecordClass = specificRecordClass;
  }

  public int getProtocolVersion() {
    return this.protocolVersion;
  }
}
