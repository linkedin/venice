package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.utils.Pair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.node.JsonNodeFactory;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.client.store.ComputeOperationType.*;


/**
 * This class is used to build a {@link ComputeRequestV1} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 * @param <K>
 */
public class AvroComputeRequestBuilder<K> implements ComputeRequestBuilder<K> {
  private static final Schema.Field VENICE_COMPUTATION_ERROR_MAP_FIELD = new Schema.Field(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME,
      Schema.createMap(Schema.create(Schema.Type.STRING)), "", JsonNodeFactory.instance.objectNode());

  private static final Map< Map<String, Object>, Pair<Schema, String>> RESULT_SCHEMA_CACHE = new ConcurrentHashMap<>();
  private static final String PROJECTION_SPEC = "projection_spec";
  private static final String DOT_PRODUCT_SPEC = "dotProduct_spec";

  private final Schema latestValueSchema;
  private final InternalAvroStoreClient storeClient;
  private final String resultSchemaName;
  private final Optional<ClientStats> stats;
  private final long preRequestTimeInNS;
  private Set<String> projectFields = new HashSet<>();
  private List<DotProduct> dotProducts = new LinkedList<>();

  public AvroComputeRequestBuilder(Schema latestValueSchema, InternalAvroStoreClient storeClient,
      Optional<ClientStats> stats, final long preRequestTimeInNS) {
    if (latestValueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceClientException("Only value schema with 'RECORD' type is supported");
    }
    if (latestValueSchema.getField(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME) != null) {
      throw new VeniceClientException("Field name: " + VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME +
          " is reserved, please don't use it in value schema: " + latestValueSchema);
    }
    this.latestValueSchema = latestValueSchema;
    this.storeClient = storeClient;
    this.stats = stats;
    this.preRequestTimeInNS = preRequestTimeInNS;
    this.resultSchemaName = storeClient.getStoreName() + "_VeniceComputeResult";
  }

  @Override
  public ComputeRequestBuilder project(String... fieldNames) throws VeniceClientException {
    for (String fieldName : fieldNames) {
      projectFields.add(fieldName);
    }

    return this;
  }

  @Override
  public ComputeRequestBuilder dotProduct(String inputFieldName, Float[] dotProductParam, String resultFieldName)
      throws VeniceClientException {
    DotProduct dotProduct = (DotProduct) DOT_PRODUCT.getNewInstance();
    dotProduct.field = inputFieldName;
    dotProduct.dotProductParam = Arrays.asList(dotProductParam);
    dotProduct.resultFieldName = resultFieldName;
    dotProducts.add(dotProduct);

    return this;
  }

  private Pair<Schema, String> getResultSchema() {
    Map<String, Object> computeSpec = new HashMap<>();
    computeSpec.put(PROJECTION_SPEC, projectFields);
    List<Pair<CharSequence, CharSequence>> dotProductPairs = new LinkedList<>();
    dotProducts.forEach( dotProduct -> {
      dotProductPairs.add(Pair.create(dotProduct.field, dotProduct.resultFieldName));
    });
    computeSpec.put(DOT_PRODUCT_SPEC, dotProductPairs);
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
      Set<CharSequence> dotProductResultFields = new HashSet<>();
      dotProducts.forEach( dotProduct -> {
        Schema.Field fieldSchema = latestValueSchema.getField(dotProduct.field.toString());
        if (null == fieldSchema) {
          throw new VeniceClientException("Unknown dotProduct field: " + dotProduct.field);
        }
        if (fieldSchema.schema().getType() != Schema.Type.ARRAY) {
          throw new VeniceClientException("DotProduct field: " + dotProduct.field + " isn't with 'ARRAY' type");
        }
        // TODO: is it necessary to be 'FLOAT' only?
        Schema elementSchema = fieldSchema.schema().getElementType();
        if (elementSchema.getType() != Schema.Type.FLOAT) {
          throw new VeniceClientException("DotProduct field: " + dotProduct.field + " isn't an 'ARRAY' of 'FLOAT'");
        }

        if (dotProductResultFields.contains(dotProduct.resultFieldName)) {
          throw new VeniceClientException("DotProduct result field: " + dotProduct.resultFieldName +
              " has been specified more than once");
        }
        if (null != latestValueSchema.getField(dotProduct.resultFieldName.toString())) {
          throw new VeniceClientException("DotProduct result field: " + dotProduct.resultFieldName +
              " collides with the fields defined in value schema");
        }
        if (VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME.equals(dotProduct.resultFieldName.toString())) {
          throw new VeniceClientException("Field name: " + dotProduct.resultFieldName +
              " is reserved, please choose a different name to store the computed result");
        }
        dotProductResultFields.add(dotProduct.resultFieldName);
      });

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
  public CompletableFuture<Map<K, GenericRecord>> execute(Set<K> keys) throws VeniceClientException {
    Pair<Schema,String> resultSchema = getResultSchema();

    // Generate ComputeRequest object
    ComputeRequestV1 computeRequest = new ComputeRequestV1();
    computeRequest.resultSchemaStr = resultSchema.getSecond();
    computeRequest.operations = new LinkedList<>();
    dotProducts.forEach( dotProduct -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = DOT_PRODUCT.getValue();
      computeOperation.operation = dotProduct;
      computeRequest.operations.add(computeOperation);
    });

    return storeClient.compute(computeRequest, keys, resultSchema.getFirst(), stats, preRequestTimeInNS);
  }
}
