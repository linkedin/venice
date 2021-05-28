package com.linkedin.venice.client.store;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * This class is used to build a {@link ComputeRequestWrapper} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 *
 * This class is package-private on purpose.
 * @param <K>
 */
public class AvroComputeRequestBuilderV2<K> extends AbstractAvroComputeRequestBuilder<K> {

  private static final int computeRequestVersion = COMPUTE_REQUEST_VERSION_V2;

  public AvroComputeRequestBuilderV2(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats) {
    this(latestValueSchema, storeClient, stats, streamingStats, new SystemTime(), false, null, null);
  }

  public AvroComputeRequestBuilderV2(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats, boolean reuseObjects,
      BinaryEncoder reusedEncoder, ByteArrayOutputStream reusedOutputStream) {
    this(latestValueSchema, storeClient, stats, streamingStats, new SystemTime(), reuseObjects, reusedEncoder, reusedOutputStream);
  }

  public AvroComputeRequestBuilderV2(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats, Time time, boolean reuseObjects,
      BinaryEncoder reusedEncoder, ByteArrayOutputStream reusedOutputStream) {
    super(latestValueSchema, storeClient, stats, streamingStats, time, reuseObjects, reusedEncoder, reusedOutputStream);
  }

  @Override
  protected ComputeRequestWrapper generateComputeRequest(String resultSchemaStr) {
    // Generate ComputeRequestWrapper object
    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(computeRequestVersion);
    computeRequestWrapper.setValueSchema(latestValueSchema);
    computeRequestWrapper.setResultSchemaStr(resultSchemaStr);
    computeRequestWrapper.setOperations(getCommonComputeOperations());

    return computeRequestWrapper;
  }

  @Override
  public ComputeRequestBuilder<K> count(String inputFieldName, String resultFieldName) {
    throw new VeniceException("Count is not supported in V2 compute request.");
  }
}
