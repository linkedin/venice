package com.linkedin.venice.spark.input.kafka.ttl;

import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.common.VeniceRmdTTLFilter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;


/**
 * Spark adapter for TTL filtering of assembled (post-chunk-assembly) records.
 *
 * It wraps the assembled Spark Row in an adapter that implements the VeniceRmdTTLFilter interface,
 * allowing reuse of the core TTL filtering logic.
 */
public class SparkChunkedPayloadTTLFilter implements Serializable, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(SparkChunkedPayloadTTLFilter.class);

  private static final long serialVersionUID = 1L;

  private transient AssembledRowWrapperTTLFilter ttlFilter;
  private final VeniceProperties props;

  public SparkChunkedPayloadTTLFilter(VeniceProperties props) {
    this.props = props;
  }

  private AssembledRowWrapperTTLFilter getTTLFilter() throws IOException {
    if (ttlFilter == null) {
      ttlFilter = new AssembledRowWrapperTTLFilter(props);
    }
    return ttlFilter;
  }

  /**
   * Filter and potentially update the assembled row based on TTL.
   *
   * This method supports PARTIALLY_UPDATED cases where some fields are stale but others are fresh.
   * It returns a FilterResult that indicates whether the row should be filtered and provides
   * the potentially modified wrapper.
   *
   * @param assembledRow Row with DEFAULT_SCHEMA_WITH_SCHEMA_ID (key, value, rmd, schema_id, rmd_version_id)
   * @return FilterResult containing filter decision and potentially modified wrapper
   */
  public FilterResult filterAndUpdate(Row assembledRow) throws IOException {
    AssembledRowWrapper wrapper = new AssembledRowWrapper(assembledRow);
    boolean shouldFilter = getTTLFilter().checkAndMaybeFilterValue(wrapper);
    return new FilterResult(shouldFilter, wrapper);
  }

  /**
   * Result of TTL filtering that includes both the filter decision and the potentially modified wrapper.
   */
  public static class FilterResult {
    private final boolean shouldFilter;
    private final AssembledRowWrapper wrapper;

    FilterResult(boolean shouldFilter, AssembledRowWrapper wrapper) {
      this.shouldFilter = shouldFilter;
      this.wrapper = wrapper;
    }

    public boolean shouldFilter() {
      return shouldFilter;
    }

    public AssembledRowWrapper getWrapper() {
      return wrapper;
    }
  }

  @Override
  public void close() {
    if (ttlFilter != null) {
      try {
        ttlFilter.close();
      } catch (Exception e) {
        LOGGER.error("Error closing TTL filter", e);
      }
    }
  }

  /**
   * Wrapper around assembled Spark Row that adapts it to the VeniceRmdTTLFilter interface.
   * The row has DEFAULT_SCHEMA_WITH_SCHEMA_ID: (key, value, rmd, schema_id, rmd_version_id)
   *
   * This wrapper supports mutation for PARTIALLY_UPDATED cases in Active-Active replication,
   * where some fields of a collection (map/list) are stale while others are fresh.
   */
  public static class AssembledRowWrapper implements Serializable {
    private static final long serialVersionUID = 1L;

    // Mutable fields to support partial TTL updates
    private byte[] value;
    private byte[] rmd;
    private final int schemaId;
    private final int rmdVersionId;

    // These are transient because ByteBuffer is not serializable
    private transient ByteBuffer valuePayload;
    private transient ByteBuffer rmdPayload;

    AssembledRowWrapper(Row row) {
      this.value = row.getAs(VALUE_COLUMN_NAME);
      this.rmd = row.getAs(RMD_COLUMN_NAME);
      this.schemaId = row.getAs(SCHEMA_ID_COLUMN_NAME);
      this.rmdVersionId = row.getAs(RMD_VERSION_ID_COLUMN_NAME);
    }

    public int getSchemaID() {
      return schemaId;
    }

    public int getReplicationMetadataVersionId() {
      return rmdVersionId;
    }

    public ByteBuffer getReplicationMetadataPayload() {
      if (rmdPayload == null && rmd != null) {
        rmdPayload = ByteBuffer.wrap(rmd);
      }
      return rmdPayload;
    }

    public ByteBuffer getValuePayload() {
      if (valuePayload == null && value != null) {
        valuePayload = ByteBuffer.wrap(value);
      }
      return valuePayload;
    }

    public byte[] getValue() {
      return value;
    }

    public byte[] getRmd() {
      return rmd;
    }

    public void setValue(byte[] value) {
      this.value = value;
      this.valuePayload = null; // Invalidate cached ByteBuffer
    }

    public void setRmd(byte[] rmd) {
      this.rmd = rmd;
      this.rmdPayload = null; // Invalidate cached ByteBuffer
    }
  }

  /**
   * TTL filter to filter chunked rows.
   */
  private static class AssembledRowWrapperTTLFilter extends VeniceRmdTTLFilter<AssembledRowWrapper> {
    AssembledRowWrapperTTLFilter(VeniceProperties props) throws IOException {
      super(props);
    }

    @Override
    protected int getSchemaId(AssembledRowWrapper wrapper) {
      return wrapper.getSchemaID();
    }

    @Override
    protected int getRmdProtocolId(AssembledRowWrapper wrapper) {
      return wrapper.getReplicationMetadataVersionId();
    }

    @Override
    protected ByteBuffer getRmdPayload(AssembledRowWrapper wrapper) {
      return wrapper.getReplicationMetadataPayload();
    }

    @Override
    protected ByteBuffer getValuePayload(AssembledRowWrapper wrapper) {
      return wrapper.getValuePayload();
    }

    @Override
    protected void updateRmdPayload(AssembledRowWrapper wrapper, ByteBuffer payload) {
      // Update RMD payload for PARTIALLY_UPDATED case (some fields stale, others fresh)
      wrapper.setRmd(payload.array());
    }

    @Override
    protected void updateValuePayload(AssembledRowWrapper wrapper, byte[] payload) {
      // Update value payload for PARTIALLY_UPDATED case (some fields stale, others fresh)
      wrapper.setValue(payload);
    }
  }
}
