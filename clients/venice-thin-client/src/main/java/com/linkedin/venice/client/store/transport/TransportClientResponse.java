package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.SchemaData;
import java.util.Arrays;


/**
 * This class is used to capture the result returned by {@link TransportClient}
 */
public class TransportClientResponse {
  private int schemaId;
  private CompressionStrategy compressionStrategy;
  private byte[] body;

  public TransportClientResponse(int schemaId, CompressionStrategy compressionStrategy, byte[] body) {
    this.schemaId = schemaId;
    this.compressionStrategy = compressionStrategy;
    this.body = body;
  }

  public int getSchemaId() {
    return this.schemaId;
  }

  public CompressionStrategy getCompressionStrategy() {
    return this.compressionStrategy;
  }

  public byte[] getBody() {
    return this.body;
  }

  public boolean isSchemaIdValid() {
    return schemaId != SchemaData.INVALID_VALUE_SCHEMA_ID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TransportClientResponse that = (TransportClientResponse) o;
    return schemaId == that.schemaId && compressionStrategy == that.compressionStrategy
        && Arrays.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    int result = schemaId;
    result = 31 * result + compressionStrategy.getValue();
    result = 31 * result + Arrays.hashCode(body);
    return result;
  }
}
