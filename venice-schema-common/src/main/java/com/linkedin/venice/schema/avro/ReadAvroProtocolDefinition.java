package com.linkedin.venice.schema.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public enum ReadAvroProtocolDefinition {
  /**
   * Client request for single-get v1.
   */
  SINGLE_GET_CLIENT_REQUEST_V1(1, Optional.empty(), Optional.empty()),

  /**
   * Router request for single-get v1.
   */
  SINGLE_GET_ROUTER_REQUEST_V1(1, Optional.empty(), Optional.empty()),

  /**
   * Client request for multi-get v1.
   */
  MULTI_GET_CLIENT_REQUEST_V1(1, Optional.empty(), Optional.of(Schema.create(Schema.Type.BYTES))),

  /**
   * Router request key for multi-get v1.
   */
  MULTI_GET_ROUTER_REQUEST_V1(1, Optional.of(MultiGetRouterRequestKeyV1.class), Optional.of(MultiGetRouterRequestKeyV1.SCHEMA$)),
  /**
   * Response record for multi-get v1.
   */
  MULTI_GET_RESPONSE_V1(1, Optional.of(MultiGetResponseRecordV1.class), Optional.of(MultiGetResponseRecordV1.SCHEMA$));

  /**
   * Current version being used.
   */
  final int protocolVersion;

  /**
   * The {@link SpecificRecord} class being used currently.
   */
  final Optional<Class<? extends SpecificRecord>> specificRecordClass;

  final Optional<Schema> schema;

  ReadAvroProtocolDefinition(int protocolVersion, Optional<Class<? extends SpecificRecord>> specificRecordClass, Optional<Schema> schema) {
    this.protocolVersion = protocolVersion;
    this.specificRecordClass = specificRecordClass;
    this.schema = schema;
  }

  public int getProtocolVersion() {
    return this.protocolVersion;
  }

  public Schema getSchema() {
    if (!this.schema.isPresent()) {
      throw new VeniceException("No defined schema for protocol: " + this);
    }
    return this.schema.get();
  }
}
