package com.linkedin.venice.spark.input.kafka.ttl;

import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceRmdTTLFilter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.spark.sql.Row;


/**
 * Spark-compatible TTL filter for repush workloads that filters records based on RMD timestamps.
 *
 * This filter wraps the core TTL filtering logic from VeniceRmdTTLFilter but makes it work with
 * Spark Rows that have RAW_PUBSUB_INPUT_TABLE_SCHEMA.
 *
 * The filter:
 * 1. Extracts schema_id, rmd_version_id, rmd_payload, and value from Spark Row
 * 2. Uses VeniceRmdTTLFilter logic to determine if record should be filtered
 * 3. Returns true if record is stale and should be removed
 *
 * This is the Spark equivalent of VeniceKafkaInputTTLFilter used in MapReduce.
 *
 * Note: This class is created per-partition in Spark's mapPartitions and does not need
 * to be serialized across the network, so the ttlFilter field is marked transient.
 */
public class SparkKafkaInputTTLFilter implements Serializable {
  private static final long serialVersionUID = 1L;

  // Column indices in RAW_PUBSUB_INPUT_TABLE_SCHEMA
  private static final int MESSAGE_TYPE_IDX = 3;
  private static final int SCHEMA_ID_IDX = 4;
  private static final int KEY_IDX = 5;
  private static final int VALUE_IDX = 6;
  private static final int RMD_VERSION_ID_IDX = 7;
  private static final int RMD_PAYLOAD_IDX = 8;

  private final transient VeniceRmdTTLFilter<RowWrapper> ttlFilter;

  public SparkKafkaInputTTLFilter(VeniceProperties props) throws IOException {
    this.ttlFilter = new RowWrapperTTLFilter(props);
  }

  /**
   * Check if a row should be filtered based on TTL policy.
   *
   * @param row Spark Row with RAW_PUBSUB_INPUT_TABLE_SCHEMA
   * @return true if the record should be filtered (is stale), false otherwise
   */
  public boolean shouldFilter(Row row) {
    // Create wrapper for this row
    RowWrapper wrapper = new RowWrapper(row);

    // Use the TTL filter to check if record should be filtered
    return ttlFilter.checkAndMaybeFilterValue(wrapper);
  }

  public void close() {
    if (ttlFilter != null) {
      ttlFilter.close();
    }
  }

  /**
   * Wrapper class to adapt Spark Row to VeniceRmdTTLFilter interface.
   * This allows us to reuse the existing TTL filtering logic.
   */
  private static class RowWrapper implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int schemaId;
    private final int rmdVersionId;
    private transient ByteBuffer rmdPayload;
    private transient ByteBuffer valuePayload;

    RowWrapper(Row row) {
      this.schemaId = row.getInt(SCHEMA_ID_IDX);
      this.rmdVersionId = row.getInt(RMD_VERSION_ID_IDX);

      byte[] rmdBytes = (byte[]) row.get(RMD_PAYLOAD_IDX);
      this.rmdPayload = rmdBytes != null ? ByteBuffer.wrap(rmdBytes) : null;

      byte[] valueBytes = (byte[]) row.get(VALUE_IDX);
      this.valuePayload = valueBytes != null ? ByteBuffer.wrap(valueBytes) : null;
    }

    int getSchemaID() {
      return schemaId;
    }

    int getRmdVersionId() {
      return rmdVersionId;
    }

    ByteBuffer getRmdPayload() {
      return rmdPayload;
    }

    ByteBuffer getValuePayload() {
      return valuePayload;
    }

    void setRmdPayload(ByteBuffer payload) {
      this.rmdPayload = payload;
    }

    void setValuePayload(byte[] payload) {
      this.valuePayload = payload != null ? ByteBuffer.wrap(payload) : null;
    }
  }

  /**
   * Concrete implementation of VeniceRmdTTLFilter for RowWrapper.
   */
  private static class RowWrapperTTLFilter extends VeniceRmdTTLFilter<RowWrapper> {
    RowWrapperTTLFilter(VeniceProperties props) throws IOException {
      super(props);
    }

    @Override
    protected int getSchemaId(RowWrapper value) {
      return value.getSchemaID();
    }

    @Override
    protected int getRmdProtocolId(RowWrapper value) {
      return value.getRmdVersionId();
    }

    @Override
    protected ByteBuffer getRmdPayload(RowWrapper value) {
      return value.getRmdPayload();
    }

    @Override
    protected ByteBuffer getValuePayload(RowWrapper value) {
      return value.getValuePayload();
    }

    @Override
    protected void updateRmdPayload(RowWrapper value, ByteBuffer payload) {
      value.setRmdPayload(payload);
    }

    @Override
    protected void updateValuePayload(RowWrapper value, byte[] payload) {
      value.setValuePayload(payload);
    }

    @Override
    protected boolean skipRmdRecord(RowWrapper value) {
      // Skip chunked records (negative schema ID)
      return value.getSchemaID() < 0;
    }
  }
}
