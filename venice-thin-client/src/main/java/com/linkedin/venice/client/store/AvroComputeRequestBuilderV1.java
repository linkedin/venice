package com.linkedin.venice.client.store;

import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Pair;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import static com.linkedin.venice.VeniceConstants.*;


/**
 * This class is used to build a {@link ComputeRequestWrapper} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 *
 * This class is package-private on purpose.
 * @param <K>
 */
class AvroComputeRequestBuilderV1<K> extends AbstractAvroComputeRequestBuilder<K> {
  private static final Schema DOT_PRODUCT_RESULT_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema COSINE_SIMILARITY_RESULT_SCHEMA = Schema.create(Schema.Type.DOUBLE);

  private static final int computeRequestVersion = COMPUTE_REQUEST_VERSION_V1;

  public AvroComputeRequestBuilderV1(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats) {
    this(latestValueSchema, storeClient, stats, streamingStats, new SystemTime());
  }

  public AvroComputeRequestBuilderV1(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats, Time time) {
    super(latestValueSchema, storeClient, stats, streamingStats, time);
  }

  protected Pair<Schema, String> getResultSchema() {
    Map<String, Object> computeSpec = getCommonComputeSpec();
    return RESULT_SCHEMA_CACHE.computeIfAbsent(computeSpec, spec -> {
      /**
       * This class delayed all the validity check here to avoid unnecessary overhead for every request
       * when application always specify the same compute operations.
       */
      // Check the validity first
      commonValidityCheck();

      // Generate result schema
      List<Schema.Field> resultSchemaFields = getCommonResultFields();
      resultSchemaFields.add(VENICE_COMPUTATION_ERROR_MAP_FIELD);

      Schema generatedResultSchema = Schema.createRecord(resultSchemaName, "", "", false);
      generatedResultSchema.setFields(resultSchemaFields);
      /**
       * TODO: we should do some optimization against the generated result schema string,
       * and here are the potential ways:
       * 1. Remove the unnecessary fields, such as 'doc';
       * 2. Compress the generated result schema;
       * 3. The ultimate optimization: dynamic result schema registry;
       */
      String generatedResultSchemaStr = generatedResultSchema.toString();

      return Pair.create(generatedResultSchema, generatedResultSchemaStr);
    });
  }

  @Override
  protected ComputeRequestWrapper generateComputeRequest(String resultSchemaStr) {
    // Generate ComputeRequestWrapper object
    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(computeRequestVersion);
    computeRequestWrapper.setResultSchemaStr(resultSchemaStr);
    List<ComputeOperation> operations = getCommonComputeOperations();
    computeRequestWrapper.setOperations((List)operations);

    return computeRequestWrapper;
  }

  @Override
  public ComputeRequestBuilder<K> hadamardProduct(String inputFieldName, List<Float> hadamardProductParam, String resultFieldName) {
    throw new VeniceException("Hadamard-product is not supported in V1 compute request.");
  }

  @Override
  protected Schema getDotProductResultSchema() {
    return DOT_PRODUCT_RESULT_SCHEMA;
  }

  @Override
  protected Schema getCosineSimilarityResultSchema() {
    return COSINE_SIMILARITY_RESULT_SCHEMA;
  }

  @Override
  protected JsonNode getDotProductResultDefaultValue() {
    return JsonNodeFactory.instance.numberNode(0);
  }

  @Override
  protected JsonNode getCosineSimilarityResultDefaultValue() {
    return JsonNodeFactory.instance.numberNode(0);
  }
}
