package com.linkedin.venice.client.store;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.COSINE_SIMILARITY;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.COUNT;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.DOT_PRODUCT;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.HADAMARD_PRODUCT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaAndToString;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.avro.Schema;


/**
 * This abstract class contains all the common field and APIs for compute request builder;
 * for each new compute request version, there will be a new builder class that extends
 * this class and the new builder will include customized fields and APIs.
 * @param <K>
 */
public abstract class AbstractAvroComputeRequestBuilder<K> implements ComputeRequestBuilder<K> {
  protected static final Map<Map<String, Object>, SchemaAndToString> RESULT_SCHEMA_CACHE =
      new VeniceConcurrentHashMap<>();
  protected static final String PROJECTION_SPEC = "projection_spec";
  protected static final String DOT_PRODUCT_SPEC = "dotProduct_spec";
  protected static final String COSINE_SIMILARITY_SPEC = "cosineSimilarity_spec";
  protected static final String HADAMARD_PRODUCT_SPEC = "hadamardProduct_spec";

  protected static final Schema HADAMARD_PRODUCT_RESULT_SCHEMA = Schema.createUnion(
      Arrays.asList(Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.FLOAT))));
  protected static final Schema DOT_PRODUCT_RESULT_SCHEMA =
      Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
  protected static final Schema COSINE_SIMILARITY_RESULT_SCHEMA =
      Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));

  protected final AvroGenericReadComputeStoreClient storeClient;
  protected final int latestValueSchemaId;
  protected final Schema latestValueSchema;
  protected final String resultSchemaName;

  private boolean executed = false;
  private Time time = new SystemTime();
  private Optional<ClientStats> streamingStats = Optional.empty();

  private boolean projectionFieldValidation = true;
  private Set<String> projectFields = new HashSet<>();
  private List<DotProduct> dotProducts = new LinkedList<>();
  private List<CosineSimilarity> cosineSimilarities = new LinkedList<>();
  private List<HadamardProduct> hadamardProducts = new LinkedList<>();

  public AbstractAvroComputeRequestBuilder(AvroGenericReadComputeStoreClient storeClient, SchemaReader schemaReader) {
    this.latestValueSchemaId = schemaReader.getLatestValueSchemaId();
    if (latestValueSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      throw new VeniceClientException("Invalid value schema ID: " + latestValueSchemaId);
    }
    this.latestValueSchema = schemaReader.getValueSchema(latestValueSchemaId);
    if (latestValueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceClientException("Only value schema with 'RECORD' type is supported");
    }
    if (latestValueSchema.getField(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME) != null) {
      throw new VeniceClientException(
          "Field name: " + VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME
              + " is reserved, please don't use it in value schema: " + latestValueSchema);
    }

    this.storeClient = storeClient;
    this.resultSchemaName =
        ComputeUtils.removeAvroIllegalCharacter(storeClient.getStoreName()) + "_VeniceComputeResult";

  }

  /** test-only*/
  public AbstractAvroComputeRequestBuilder<K> setTime(Time time) {
    this.time = time;
    return this;
  }

  public AbstractAvroComputeRequestBuilder<K> setStats(Optional<ClientStats> streamingStats) {
    this.streamingStats = streamingStats;
    return this;
  }

  public AbstractAvroComputeRequestBuilder<K> setValidateProjectionFields(boolean projectionFieldValidation) {
    this.projectionFieldValidation = projectionFieldValidation;
    return this;
  }

  @Override
  public ComputeRequestBuilder<K> project(String... fieldNames) throws VeniceClientException {
    return project(Arrays.asList(fieldNames));
  }

  @Override
  public ComputeRequestBuilder<K> project(Collection<String> fieldNames) throws VeniceClientException {
    projectFields.addAll(fieldNames);
    return this;
  }

  @Override
  public ComputeRequestBuilder<K> dotProduct(String inputFieldName, List<Float> dotProductParam, String resultFieldName)
      throws VeniceClientException {
    DotProduct dotProduct = (DotProduct) DOT_PRODUCT.getNewInstance();
    dotProduct.field = inputFieldName;
    dotProduct.dotProductParam = (dotProductParam == null ? Collections.emptyList() : dotProductParam);
    dotProduct.resultFieldName = resultFieldName;
    dotProducts.add(dotProduct);

    return this;
  }

  @Override
  public ComputeRequestBuilder<K> cosineSimilarity(
      String inputFieldName,
      List<Float> cosSimilarityParam,
      String resultFieldName) throws VeniceClientException {
    CosineSimilarity cosineSimilarity = (CosineSimilarity) COSINE_SIMILARITY.getNewInstance();
    cosineSimilarity.field = inputFieldName;
    cosineSimilarity.cosSimilarityParam = (cosSimilarityParam == null ? Collections.emptyList() : cosSimilarityParam);
    cosineSimilarity.resultFieldName = resultFieldName;
    cosineSimilarities.add(cosineSimilarity);

    return this;
  }

  /**
   * Generate compute spec for projections, dot-product and cosine-similarity.
   * @return common compute spec
   */
  protected Map<String, Object> getCommonComputeSpec() {
    Map<String, Object> computeSpec = new HashMap<>();
    computeSpec.put(PROJECTION_SPEC, projectFields);
    List<Pair<CharSequence, CharSequence>> dotProductPairs = new LinkedList<>();
    dotProducts.forEach(dotProduct -> {
      dotProductPairs.add(Pair.create(dotProduct.field, dotProduct.resultFieldName));
    });
    computeSpec.put(DOT_PRODUCT_SPEC, dotProductPairs);
    List<Pair<CharSequence, CharSequence>> cosineSimilarityPairs = new LinkedList<>();
    cosineSimilarities.forEach(cosineSimilarity -> {
      cosineSimilarityPairs.add(Pair.create(cosineSimilarity.field, cosineSimilarity.resultFieldName));
    });
    computeSpec.put(COSINE_SIMILARITY_SPEC, cosineSimilarityPairs);
    List<Pair<CharSequence, CharSequence>> hadamardProductPairs = new LinkedList<>();
    hadamardProducts.forEach(hadamardProduct -> {
      hadamardProductPairs.add(Pair.create(hadamardProduct.field, hadamardProduct.resultFieldName));
    });
    computeSpec.put(HADAMARD_PRODUCT_SPEC, hadamardProductPairs);
    return computeSpec;
  }

  /**
   * Compute operation validity check for projections, dot-product and cosine-similarity.
   * @return a set of existing operations result field name
   */
  protected Set<String> commonValidityCheck() {
    // Projection
    if (projectionFieldValidation) {
      projectFields.forEach(projectField -> {
        if (latestValueSchema.getField(projectField) == null) {
          throw new VeniceClientException("Unknown project field: " + projectField);
        }
      });
    }
    // DotProduct
    Set<String> computeResultFields = new HashSet<>();
    dotProducts.forEach(
        dotProduct -> checkComputeFieldValidity(
            dotProduct.field.toString(),
            dotProduct.resultFieldName.toString(),
            computeResultFields,
            DOT_PRODUCT));
    // CosineSimilarity
    /**
     * Use the same compute result field set because the result field name in cosine similarity couldn't collide with
     * the result field name in dot product
     */
    cosineSimilarities.forEach(
        cosineSimilarity -> checkComputeFieldValidity(
            cosineSimilarity.field.toString(),
            cosineSimilarity.resultFieldName.toString(),
            computeResultFields,
            COSINE_SIMILARITY));
    // HadamardProduct
    hadamardProducts.forEach(
        hadamardProduct -> checkComputeFieldValidity(
            hadamardProduct.field.toString(),
            hadamardProduct.resultFieldName.toString(),
            computeResultFields,
            HADAMARD_PRODUCT));

    return computeResultFields;
  }

  /**
   * Generate compute result schema fields for projections, dot-product and cosine-similarity.
   * @return a list of existing compute result schema fields
   */
  protected List<Schema.Field> getCommonResultFields() {
    List<Schema.Field> resultSchemaFields = new LinkedList<>();
    projectFields.forEach(projectField -> {
      Schema.Field existingField = latestValueSchema.getField(projectField);
      /**
       * {@link AvroCompatibilityHelper#newField} is behaving differently with different Avro versions:
       * 1. For Avro-1.8 or below, this method will guarantee the cloned field will be exactly same as the existing field.
       * 2. For Avro-1.9 or above, this method couldn't guarantee the cloned field will be exactly same as the existing field
       *    since the way to extract the default value from the existing field changes and we could only extract the default
       *    value in Java format, but the {@link Schema.Field#equals} will compare the underlying {@link com.fasterxml.jackson.databind.JsonNode}
       *    format of the default value.
       */
      if (existingField != null) {
        resultSchemaFields.add(AvroCompatibilityHelper.newField(existingField).setDoc("").build());
      }
    });
    dotProducts.forEach(dotProduct -> {
      Schema.Field dotProductField = AvroCompatibilityHelper
          .createSchemaField(dotProduct.resultFieldName.toString(), DOT_PRODUCT_RESULT_SCHEMA, "", null);
      resultSchemaFields.add(dotProductField);
    });
    cosineSimilarities.forEach(cosineSimilarity -> {
      Schema.Field cosineSimilarityField = AvroCompatibilityHelper
          .createSchemaField(cosineSimilarity.resultFieldName.toString(), COSINE_SIMILARITY_RESULT_SCHEMA, "", null);
      resultSchemaFields.add(cosineSimilarityField);
    });
    hadamardProducts.forEach(hadamardProduct -> {
      Schema.Field hadamardProductField = AvroCompatibilityHelper
          .createSchemaField(hadamardProduct.resultFieldName.toString(), HADAMARD_PRODUCT_RESULT_SCHEMA, "", null);
      resultSchemaFields.add(hadamardProductField);
    });
    /**
     * Error map field can not be a static variable; after setting the error map field in a schema, the position of the
     * field will be updated, so the next time when we set the field in a new schema, it would fail because
     * {@link Schema#setFields(List)} check whether the position is -1.
     */
    resultSchemaFields.add(
        AvroCompatibilityHelper.createSchemaField(
            VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME,
            Schema.createMap(Schema.create(Schema.Type.STRING)),
            "",
            null));
    return resultSchemaFields;
  }

  /**
   * Generate compute operations for projections, dot-product and cosine-similarity.
   * @return a list of existing compute operations
   */
  protected List<ComputeOperation> getComputeRequestOperations() {
    List<ComputeOperation> operations = new LinkedList<>();
    dotProducts.forEach(dotProduct -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = DOT_PRODUCT.getValue();
      computeOperation.operation = dotProduct;
      operations.add(computeOperation);
    });
    cosineSimilarities.forEach(cosineSimilarity -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = COSINE_SIMILARITY.getValue();
      computeOperation.operation = cosineSimilarity;
      operations.add(computeOperation);
    });
    hadamardProducts.forEach(hadamardProduct -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = HADAMARD_PRODUCT.getValue();
      computeOperation.operation = hadamardProduct;
      operations.add(computeOperation);
    });
    return operations;
  }

  @Override
  public CompletableFuture<Map<K, ComputeGenericRecord>> execute(Set<K> keys) throws VeniceClientException {
    CompletableFuture<Map<K, ComputeGenericRecord>> resultFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResponseMap<K, ComputeGenericRecord>> streamResultFuture =
        streamingExecuteInternal(keys, false);
    streamResultFuture.whenComplete((response, throwable) -> {
      if (throwable != null) {
        resultFuture.completeExceptionally(throwable);
      } else if (!response.isFullResponse()) {
        resultFuture.completeExceptionally(
            new VeniceClientException(
                "Received partial response, returned entry count: " + response.getTotalEntryCount()
                    + ", and key count: " + keys.size()));
      } else {
        resultFuture.complete(response);
      }
    });

    if (streamingStats.isPresent()) {
      return AppTimeOutTrackingCompletableFuture.track(resultFuture, streamingStats.get());
    }
    return resultFuture;
  }

  @Override
  public CompletableFuture<VeniceResponseMap<K, ComputeGenericRecord>> streamingExecute(Set<K> keys) {
    return streamingExecuteInternal(keys, true);
  }

  private CompletableFuture<VeniceResponseMap<K, ComputeGenericRecord>> streamingExecuteInternal(
      Set<K> keys,
      boolean originallyStreaming) {
    Map<K, ComputeGenericRecord> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();
    VeniceResponseCompletableFuture<VeniceResponseMap<K, ComputeGenericRecord>> resultFuture =
        new VeniceResponseCompletableFuture<>(
            () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
            keys.size(),
            streamingStats);
    streamingExecuteInternal(keys, originallyStreaming, new StreamingCallback<K, ComputeGenericRecord>() {
      @Override
      public void onRecordReceived(K key, ComputeGenericRecord value) {
        if (value != null) {
          /**
           * {@link java.util.concurrent.ConcurrentHashMap#put} won't take 'null' as the value.
           */
          resultMap.put(key, value);
        } else {
          nonExistingKeyList.add(key);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        if (exception.isPresent()) {
          resultFuture.completeExceptionally(exception.get());
        } else {
          boolean isFullResponse = resultMap.size() + nonExistingKeyList.size() == keys.size();
          resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, isFullResponse));
        }
      }
    });

    return resultFuture;
  }

  @Override
  public void streamingExecute(Set<K> keys, StreamingCallback<K, ComputeGenericRecord> callback)
      throws VeniceClientException {
    streamingExecuteInternal(keys, true, callback);
  }

  private void streamingExecuteInternal(
      Set<K> keys,
      boolean originallyStreaming,
      StreamingCallback<K, ComputeGenericRecord> callback) throws VeniceClientException {
    if (executed) {
      throw new VeniceClientException(getClass().getName() + " reuse is not supported.");
    }
    executed = true;

    long preRequestTimeInNS = time.nanoseconds();
    SchemaAndToString resultSchema = getResultSchema();
    // Generate ComputeRequest object
    ComputeRequestWrapper computeRequestWrapper = generateComputeRequest(resultSchema, originallyStreaming);
    storeClient.compute(computeRequestWrapper, keys, resultSchema.getSchema(), callback, preRequestTimeInNS);
  }

  protected void checkComputeFieldValidity(
      String computeFieldName,
      String resultFieldName,
      Set<String> resultFieldsSet,
      ComputeOperationType computeType) {
    final Schema.Field fieldSchema = latestValueSchema.getField(computeFieldName);
    if (fieldSchema == null) {
      throw new VeniceClientException("Unknown " + computeType + " field: " + computeFieldName);
    }

    final Schema.Type fieldType = fieldSchema.schema().getType();
    if (computeType == COUNT) {
      if (fieldType != Schema.Type.ARRAY && fieldType != Schema.Type.MAP) {
        throw new VeniceClientException(computeType + " field: " + computeFieldName + " isn't 'ARRAY' or 'MAP' type");
      }
    } else {
      if (fieldType == Schema.Type.ARRAY) {
        // TODO: is it necessary to be 'FLOAT' only?
        Schema elementSchema = fieldSchema.schema().getElementType();
        if (elementSchema.getType() != Schema.Type.FLOAT) {
          throw new VeniceClientException(computeType + " field: " + computeFieldName + " isn't an 'ARRAY' of 'FLOAT'");
        }
      } else if (!isFieldNullableList(fieldSchema)) {
        throw new VeniceClientException(
            computeType + " field: " + computeFieldName + " isn't an 'ARRAY' type. Got: "
                + fieldSchema.schema().getType());
      }
    }

    if (resultFieldsSet.contains(resultFieldName)) {
      throw new VeniceClientException(
          computeType + " result field: " + resultFieldName + " has been specified more than once");
    }
    if (latestValueSchema.getField(resultFieldName) != null) {
      throw new VeniceClientException(
          computeType + " result field: " + resultFieldName + " collides with the fields defined in value schema");
    }
    if (VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME.equals(resultFieldName)) {
      throw new VeniceClientException(
          "Field name: " + resultFieldName
              + " is reserved, please choose a different name to store the computed result");
    }
    resultFieldsSet.add(resultFieldName);
  }

  private boolean isFieldNullableList(Schema.Field fieldSchema) {
    if (fieldSchema.schema().getType() != Schema.Type.UNION) {
      return false;
    }

    // Should have 2 parts, a NULL schema and an ARRAY schema respectively.
    List<Schema> schemas = fieldSchema.schema().getTypes();
    if (schemas.size() != 2) {
      return false;
    }
    Schema expectedNullSchema;
    Schema expectedListSchema;
    if (schemas.get(0).getType() == Schema.Type.NULL) {
      expectedNullSchema = schemas.get(0);
      expectedListSchema = schemas.get(1);
    } else {
      expectedNullSchema = schemas.get(1);
      expectedListSchema = schemas.get(0);
    }

    if (expectedNullSchema.getType() != Schema.Type.NULL) {
      return false;
    }
    if (expectedListSchema.getType() != Schema.Type.ARRAY) {
      return false;
    }
    // Make sure it is a list of float specifically
    return expectedListSchema.getElementType().getType() == Schema.Type.FLOAT;
  }

  protected SchemaAndToString getResultSchema() {
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
      Schema generatedResultSchema = Schema.createRecord(resultSchemaName, "", "", false);
      generatedResultSchema.setFields(resultSchemaFields);
      return new SchemaAndToString(generatedResultSchema);
    });
  }

  @Override
  public ComputeRequestBuilder<K> hadamardProduct(
      String inputFieldName,
      List<Float> hadamardProductParam,
      String resultFieldName) throws VeniceClientException {
    HadamardProduct hadamardProduct = (HadamardProduct) HADAMARD_PRODUCT.getNewInstance();
    hadamardProduct.field = inputFieldName;
    hadamardProduct.hadamardProductParam =
        (hadamardProductParam == null ? Collections.emptyList() : hadamardProductParam);
    hadamardProduct.resultFieldName = resultFieldName;
    hadamardProducts.add(hadamardProduct);

    return this;
  }

  protected ComputeRequestWrapper generateComputeRequest(SchemaAndToString resultSchema, boolean originallyStreaming) {
    return new ComputeRequestWrapper(
        latestValueSchemaId,
        latestValueSchema,
        resultSchema.getSchema(),
        resultSchema.getToString(),
        getComputeRequestOperations(),
        originallyStreaming);
  }
}
