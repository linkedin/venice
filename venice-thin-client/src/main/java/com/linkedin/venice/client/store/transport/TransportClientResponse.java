package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.schema.SchemaData;

import java.util.Arrays;

/**
 * This class is used to capture the result returned by {@link TransportClient}
 */
public class TransportClientResponse {
  private int schemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
  private byte[] body;

  public TransportClientResponse(int schemaId, byte[] body) {
    this.schemaId = schemaId;
    this.body = body;
  }

  public int getSchemaId() {
    return this.schemaId;
  }

  public byte[] getBody() {
    return this.body;
  }

  public boolean isSchemaIdValid() {
    return schemaId != SchemaData.INVALID_VALUE_SCHEMA_ID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransportClientResponse that = (TransportClientResponse) o;

    if (schemaId != that.schemaId) return false;
    return Arrays.equals(body, that.body);

  }

  @Override
  public int hashCode() {
    int result = schemaId;
    result = 31 * result + Arrays.hashCode(body);
    return result;
  }
}
