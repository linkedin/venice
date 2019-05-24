package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.utils.ComputeUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.Collection;
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
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.node.JsonNodeFactory;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.*;


/**
 * This class is used to build a {@link ComputeRequestV1} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 *
 * This class is package-private on purpose.
 * @param <K>
 */
class AvroComputeRequestBuilder<K> implements ComputeRequestBuilder<K> {
  /**
   * Error map field can not be a static variable; after setting the error map field in a schema, the position of the
   * field will be updated, so the next time when we set the field in a new schema, it would fail because
   * {@link Schema#setFields(List)} check whether the position is -1.
   */
  private final Schema.Field VENICE_COMPUTATION_ERROR_MAP_FIELD = new Schema.Field(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME,
      Schema.createMap(Schema.create(Schema.Type.STRING)), "", JsonNodeFactory.instance.objectNode());

  private static final Map< Map<String, Object>, Pair<Schema, String>> RESULT_SCHEMA_CACHE = new VeniceConcurrentHashMap<>();
  private static final String PROJECTION_SPEC = "projection_spec";
  private static final String DOT_PRODUCT_SPEC = "dotProduct_spec";
  private static final String COSINE_SIMILARITY_SPEC = "cosineSimilarity_spec";

  private final Time time;
  private final Schema latestValueSchema;
  private final InternalAvroStoreClient storeClient;
  private final String resultSchemaName;
  private final Optional<ClientStats> stats;
  private final Optional<ClientStats> streamingStats;
  private Set<String> projectFields = new HashSet<>();
  private List<DotProduct> dotProducts = new LinkedList<>();
  private List<CosineSimilarity> cosineSimilarities = new LinkedList<>();

  public AvroComputeRequestBuilder(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats) {
    this(latestValueSchema, storeClient, stats, streamingStats, new SystemTime());
  }

  public AvroComputeRequestBuilder(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, Optional<ClientStats> streamingStats, Time time) {
    if (latestValueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceClientException("Only value schema with 'RECORD' type is supported");
    }
    if (latestValueSchema.getField(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME) != null) {
      throw new VeniceClientException("Field name: " + VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME +
          " is reserved, please don't use it in value schema: " + latestValueSchema);
    }
    this.time = time;
    this.latestValueSchema = latestValueSchema;
    this.storeClient = storeClient;
    this.stats = stats;
    this.streamingStats = streamingStats;
    this.resultSchemaName = ComputeUtils.removeAvroIllegalCharacter(storeClient.getStoreName()) + "_VeniceComputeResult";
  }

  @Override
  public ComputeRequestBuilder<K> project(String... fieldNames) throws VeniceClientException {
    for (String fieldName : fieldNames) {
      projectFields.add(fieldName);
    }

    return this;
  }

  @Override
  public ComputeRequestBuilder<K> project(Collection<String> fieldNames) throws VeniceClientException {
    for (String fieldName : fieldNames) {
      projectFields.add(fieldName);
    }

    return this;
  }

  @Override
  public ComputeRequestBuilder<K> dotProduct(String inputFieldName, List<Float> dotProductParam, String resultFieldName)
      throws VeniceClientException {
    DotProduct dotProduct = (DotProduct) DOT_PRODUCT.getNewInstance();
    dotProduct.field = inputFieldName;
    dotProduct.dotProductParam = dotProductParam;
    dotProduct.resultFieldName = resultFieldName;
    dotProducts.add(dotProduct);

    return this;
  }

  @Override
  public ComputeRequestBuilder<K> cosineSimilarity(String inputFieldName, List<Float> cosSimilarityParam, String resultFieldName)
      throws VeniceClientException {
    CosineSimilarity cosineSimilarity = (CosineSimilarity) COSINE_SIMILARITY.getNewInstance();
    cosineSimilarity.field = inputFieldName;
    cosineSimilarity.cosSimilarityParam = cosSimilarityParam;
    cosineSimilarity.resultFieldName = resultFieldName;
    cosineSimilarities.add(cosineSimilarity);

    return this;
  }

  protected Pair<Schema, String> getResultSchema() {
    Map<String, Object> computeSpec = new HashMap<>();
    computeSpec.put(PROJECTION_SPEC, projectFields);
    List<Pair<CharSequence, CharSequence>> dotProductPairs = new LinkedList<>();
    dotProducts.forEach( dotProduct -> {
      dotProductPairs.add(Pair.create(dotProduct.field, dotProduct.resultFieldName));
    });
    computeSpec.put(DOT_PRODUCT_SPEC, dotProductPairs);
    List<Pair<CharSequence, CharSequence>> cosineSimilarityPairs = new LinkedList<>();
    cosineSimilarities.forEach( cosineSimilarity -> {
      cosineSimilarityPairs.add(Pair.create(cosineSimilarity.field, cosineSimilarity.resultFieldName));
    });
    computeSpec.put(COSINE_SIMILARITY_SPEC, cosineSimilarityPairs);
    return RESULT_SCHEMA_CACHE.computeIfAbsent(computeSpec, spec -> {
      /**
       * This class delayed all the validity check here to avoid unnecessary overhead for every request
       * when application always specify the same compute operations.
       */
      // Check the validity first
      // Projection
      projectFields.forEach( projectField -> {
        if (null == latestValueSchema.getField(projectField)) {
          throw new VeniceClientException("Unknown project field: " + projectField);
        }
      });
      // DotProduct
      Set<String> computeResultFields = new HashSet<>();
      dotProducts.forEach( dotProduct ->
          checkComputeFieldValidity(dotProduct.field.toString(),
                                    dotProduct.resultFieldName.toString(),
                                    computeResultFields,
                                    DOT_PRODUCT));
      // CosineSimilarity
      /**
       * Use the same compute result field set because the result field name in cosine similarity couldn't collide with
       * the result field name in dot product
       */
      cosineSimilarities.forEach( cosineSimilarity ->
          checkComputeFieldValidity(cosineSimilarity.field.toString(),
                                    cosineSimilarity.resultFieldName.toString(),
                                    computeResultFields,
                                    COSINE_SIMILARITY));

      // Generate result schema
      List<Schema.Field> resultSchemaFields = new LinkedList<>();
      projectFields.forEach( projectField -> {
        Schema.Field existingField = latestValueSchema.getField(projectField);
        resultSchemaFields.add(new Schema.Field(existingField.name(), existingField.schema(), "", existingField.defaultValue()));
      });
      dotProducts.forEach( dotProduct -> {
        Schema.Field dotProductField = new Schema.Field(dotProduct.resultFieldName.toString(),
            Schema.create(Schema.Type.DOUBLE), "", JsonNodeFactory.instance.numberNode(0));
        resultSchemaFields.add(dotProductField);
      });
      cosineSimilarities.forEach( cosineSimilarity -> {
        Schema.Field cosineSimilarityField = new Schema.Field(cosineSimilarity.resultFieldName.toString(),
            Schema.create(Schema.Type.DOUBLE), "", JsonNodeFactory.instance.numberNode(0));
        resultSchemaFields.add(cosineSimilarityField);
      });
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

  private ComputeRequestV1 generateComputeRequest(String resultSchemaStr) {
    // Generate ComputeRequest object
    ComputeRequestV1 computeRequest = new ComputeRequestV1();
    computeRequest.resultSchemaStr = resultSchemaStr;
    computeRequest.operations = new LinkedList<>();
    dotProducts.forEach( dotProduct -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = DOT_PRODUCT.getValue();
      computeOperation.operation = dotProduct;
      computeRequest.operations.add(computeOperation);
    });
    cosineSimilarities.forEach( cosineSimilarity -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = COSINE_SIMILARITY.getValue();
      computeOperation.operation = cosineSimilarity;
      computeRequest.operations.add(computeOperation);
    });

    return computeRequest;
  }

  @Override
  public CompletableFuture<Map<K, GenericRecord>> execute(Set<K> keys) throws VeniceClientException {
    long preRequestTimeInNS = time.nanoseconds();
    Pair<Schema,String> resultSchema = getResultSchema();
    // Generate ComputeRequest object
    ComputeRequestV1 computeRequest = generateComputeRequest(resultSchema.getSecond());

    CompletableFuture<Map<K, GenericRecord>> computeFuture = storeClient.compute(computeRequest, keys,
        resultSchema.getFirst(), stats, preRequestTimeInNS);
    if (stats.isPresent()) {
      return AppTimeOutTrackingCompletableFuture.track(computeFuture, stats.get());
    }
    return computeFuture;
  }


  @Override
  public CompletableFuture<VeniceResponseMap<K, GenericRecord>> streamingExecute(Set<K> keys) {
    Map<K, GenericRecord> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();
    VeniceResponseCompletableFuture<VeniceResponseMap<K, GenericRecord>>
        resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
        keys.size(),
        streamingStats);
    streamingExecute(keys, new StreamingCallback<K, GenericRecord>() {
      @Override
      public void onRecordReceived(K key, GenericRecord value) {
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
          resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, true));
        }
      }
    });

    return resultFuture;
  }

  @Override
  public void streamingExecute(Set<K> keys, StreamingCallback<K, GenericRecord> callback) throws VeniceClientException {
    long preRequestTimeInNS = time.nanoseconds();
    Pair<Schema,String> resultSchema = getResultSchema();
    // Generate ComputeRequest object
    ComputeRequestV1 computeRequest = generateComputeRequest(resultSchema.getSecond());

    storeClient.compute(computeRequest, keys, resultSchema.getFirst(), callback, preRequestTimeInNS);
  }

  private void checkComputeFieldValidity(String computeFieldName, String resultFieldName, Set<String> resultFieldsSet, ComputeOperationType computeType) {
    Schema.Field fieldSchema = latestValueSchema.getField(computeFieldName);
    if (null == fieldSchema) {
      throw new VeniceClientException("Unknown " + computeType + " field: " + computeFieldName);
    }
    if (fieldSchema.schema().getType() != Schema.Type.ARRAY) {
      throw new VeniceClientException(computeType + " field: " + computeFieldName + " isn't with 'ARRAY' type");
    }
    // TODO: is it necessary to be 'FLOAT' only?
    Schema elementSchema = fieldSchema.schema().getElementType();
    if (elementSchema.getType() != Schema.Type.FLOAT) {
      throw new VeniceClientException(computeType + " field: " + computeFieldName + " isn't an 'ARRAY' of 'FLOAT'");
    }

    if (resultFieldsSet.contains(resultFieldName)) {
      throw new VeniceClientException(computeType + " result field: " + resultFieldName +
          " has been specified more than once");
    }
    if (null != latestValueSchema.getField(resultFieldName)) {
      throw new VeniceClientException(computeType + " result field: " + resultFieldName +
          " collides with the fields defined in value schema");
    }
    if (VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME.equals(resultFieldName)) {
      throw new VeniceClientException("Field name: " + resultFieldName +
          " is reserved, please choose a different name to store the computed result");
    }
    resultFieldsSet.add(resultFieldName);
  }
}
