package com.linkedin.venice.schema.avro;

import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV2;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV3;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV4;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


/**
 * TDOO: Consider merging with AvroProtocolDefinition
 */
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
  MULTI_GET_ROUTER_REQUEST_V1(
      1, Optional.of(MultiGetRouterRequestKeyV1.class), Optional.of(MultiGetRouterRequestKeyV1.SCHEMA$)
  ),

  /**
   * Response record for multi-get v1.
   */
  MULTI_GET_RESPONSE_V1(1, Optional.of(MultiGetResponseRecordV1.class), Optional.of(MultiGetResponseRecordV1.SCHEMA$)),

  /**
   * Compute request client key v1.
   */
  COMPUTE_REQUEST_CLIENT_KEY_V1(1, Optional.empty(), Optional.of(Schema.create(Schema.Type.BYTES))),

  /**
   * Compute request v1.
   *
   * Compute requests sent from client to router contain 2 parts; the first part contains information like an array of
   * operations and the result schema, represented by the following schema; the second part contains raw bytes of all
   * the keys that will be computed on, represented by COMPUTE_REQUEST_CLIENT_KEY_V1.
   */
  COMPUTE_REQUEST_V1(1, Optional.of(ComputeRequestV1.class), Optional.of(ComputeRequestV1.SCHEMA$)),

  /**
   * Compute request v2.
   *
   * Version 2 includes support for Hadamard Product.
   */
  COMPUTE_REQUEST_V2(2, Optional.of(ComputeRequestV2.class), Optional.of(ComputeRequestV2.SCHEMA$)),

  /**
   * Compute request v3. Its includes support for count operator.
   */
  COMPUTE_REQUEST_V3(3, Optional.of(ComputeRequestV3.class), Optional.of(ComputeRequestV3.SCHEMA$)),

  /**
   * Compute request v4. It includes support for Execute with Filter
   */
  COMPUTE_REQUEST_V4(4, Optional.of(ComputeRequestV4.class), Optional.of(ComputeRequestV4.SCHEMA$)),

  /**
   * Response record for compute v1
   */
  COMPUTE_RESPONSE_V1(1, Optional.of(ComputeResponseRecordV1.class), Optional.of(ComputeResponseRecordV1.SCHEMA$)),

  /**
   * Router request key for read compute v1.
   */
  COMPUTE_ROUTER_REQUEST_V1(
      1, Optional.of(ComputeRouterRequestKeyV1.class), Optional.of(ComputeRouterRequestKeyV1.SCHEMA$)
  );

  /**
   * Current version being used.
   */
  final int protocolVersion;

  /**
   * The {@link SpecificRecord} class being used currently.
   */
  final Optional<Class<? extends SpecificRecord>> specificRecordClass;

  final Optional<Schema> schema;

  ReadAvroProtocolDefinition(
      int protocolVersion,
      Optional<Class<? extends SpecificRecord>> specificRecordClass,
      Optional<Schema> schema) {
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
