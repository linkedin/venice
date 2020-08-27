package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.utils.Pair;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.*;


/**
 * This class is used to build a {@link ComputeRequestWrapper} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 *
 * This class is package-private on purpose.
 * @param <K>
 */
class AvroComputeRequestBuilderV2<K> extends AbstractAvroComputeRequestBuilder<K> {
  private static final String HADAMARD_PRODUCT_SPEC = "hadamardProduct_spec";

  private static final Schema DOT_PRODUCT_RESULT_SCHEMA = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
  private static final Schema COSINE_SIMILARITY_RESULT_SCHEMA = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
  private static final Schema HADAMARD_PRODUCT_RESULT_SCHEMA = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.FLOAT))));

  private List<HadamardProduct> hadamardProducts = new LinkedList<>();
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
  public ComputeRequestBuilder<K> hadamardProduct(String inputFieldName, List<Float> hadamardProductParam, String resultFieldName)
      throws VeniceClientException {
    HadamardProduct hadamardProduct = (HadamardProduct) HADAMARD_PRODUCT.getNewInstance();
    hadamardProduct.field = inputFieldName;
    hadamardProduct.hadamardProductParam = hadamardProductParam;
    hadamardProduct.resultFieldName = resultFieldName;
    hadamardProducts.add(hadamardProduct);

    return this;
  }

  @Override
  protected Pair<Schema, String> getResultSchema() {
    Map<String, Object> computeSpec = getCommonComputeSpec();
    List<Pair<CharSequence, CharSequence>> hadamardProductPairs = new LinkedList<>();
    hadamardProducts.forEach( hadamardProduct -> {
      hadamardProductPairs.add(Pair.create(hadamardProduct.field, hadamardProduct.resultFieldName));
    });
    computeSpec.put(HADAMARD_PRODUCT_SPEC, hadamardProductPairs);

    return RESULT_SCHEMA_CACHE.computeIfAbsent(computeSpec, spec -> {
      /**
       * This class delayed all the validity check here to avoid unnecessary overhead for every request
       * when application always specify the same compute operations.
       */
      // Check the validity first
      Set<String> computeResultFields = commonValidityCheck();

      // HadamardProduct
      hadamardProducts.forEach(hadamardProduct ->
          checkComputeFieldValidity(hadamardProduct.field.toString(),
                                    hadamardProduct.resultFieldName.toString(),
                                    computeResultFields,
                                    HADAMARD_PRODUCT));


      // Generate result schema
      List<Schema.Field> resultSchemaFields = getCommonResultFields();
      hadamardProducts.forEach( hadamardProduct -> {
        Schema.Field hadamardProductField = new Schema.Field(hadamardProduct.resultFieldName.toString(),
                                                             HADAMARD_PRODUCT_RESULT_SCHEMA,
                                                             "",
                                                             JsonNodeFactory.instance.nullNode());
        resultSchemaFields.add(hadamardProductField);
      });
      resultSchemaFields.add(VENICE_COMPUTATION_ERROR_MAP_FIELD);

      Schema generatedResultSchema = Schema.createRecord(resultSchemaName, "", "", false);
      generatedResultSchema.setFields(resultSchemaFields);
      return Pair.create(generatedResultSchema, generatedResultSchema.toString());
    });
  }

  @Override
  protected ComputeRequestWrapper generateComputeRequest(String resultSchemaStr) {
    // Generate ComputeRequestWrapper object
    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(computeRequestVersion);
    computeRequestWrapper.setResultSchemaStr(resultSchemaStr);
    List<ComputeOperation> operations = getCommonComputeOperations();
    hadamardProducts.forEach( hadamardProduct -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = HADAMARD_PRODUCT.getValue();
      computeOperation.operation = hadamardProduct;
      operations.add(computeOperation);
    });
    computeRequestWrapper.setOperations((List)operations);

    return computeRequestWrapper;
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
    return JsonNodeFactory.instance.nullNode();
  }

  @Override
  protected JsonNode getCosineSimilarityResultDefaultValue() {
    return JsonNodeFactory.instance.nullNode();
  }
}
