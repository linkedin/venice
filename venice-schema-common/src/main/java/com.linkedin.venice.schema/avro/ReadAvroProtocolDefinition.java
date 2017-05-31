package com.linkedin.venice.schema.avro;

import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKey;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecord;
import org.apache.avro.specific.SpecificRecord;

public enum ReadAvroProtocolDefinition {
  /**
   * Router request key for multi-get.
   */
  MULTI_GET_ROUTER_REQUEST(1, MultiGetRouterRequestKey.class),
  /**
   * Response record for multi-get.
   */
  MULTI_GET_RESPONSE(1, MultiGetResponseRecord.class);

  /**
   * Current version being used.
   */
  final int currentProtocolVersion;

  /**
   * The {@link SpecificRecord} class being used currently.
   */
  final Class<? extends SpecificRecord> specificRecordClass;

  ReadAvroProtocolDefinition(int currentProtocolVersion, Class<? extends SpecificRecord> specificRecordClass) {
    this.currentProtocolVersion = currentProtocolVersion;
    this.specificRecordClass = specificRecordClass;
  }

  public int getCurrentProtocolVersion() {
    return this.currentProtocolVersion;
  }

  public Class<? extends SpecificRecord> getSpecificRecordClass() {
    return this.specificRecordClass;
  }
}
