package com.linkedin.venice.client.store;

import static com.linkedin.venice.VeniceConstants.COMPUTE_REQUEST_VERSION_V3;
import static com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType.COUNT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.Count;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Pair;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is used to build a {@link ComputeRequestWrapper} object according to the specification,
 * and this class will invoke {@link AbstractAvroStoreClient} to send the 'compute' request to
 * backend.
 *
 * This class is package-private on purpose.
 * @param <K>
 */
public class AvroComputeRequestBuilderV3<K> extends AbstractAvroComputeRequestBuilder<K> {
  private static final int COMPUTE_REQUEST_VERSION = COMPUTE_REQUEST_VERSION_V3;
  private static final String COUNT_SPEC = "count_spec";
  private static final Schema COUNT_RESULT_SCHEMA =
      Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
  private final List<Count> countOperations = new LinkedList<>();

  public AvroComputeRequestBuilderV3(AvroGenericReadComputeStoreClient storeClient, Schema latestValueSchema) {
    super(storeClient, latestValueSchema);
  }

  @Override
  protected Pair<Schema, String> getResultSchema() {
    Map<String, Object> computeSpec = getCommonComputeSpec();

    List<Pair<CharSequence, CharSequence>> countPairs = new LinkedList<>();
    countOperations.forEach(count -> {
      countPairs.add(Pair.create(count.field, count.resultFieldName));
    });
    computeSpec.put(COUNT_SPEC, countPairs);

    return RESULT_SCHEMA_CACHE.computeIfAbsent(computeSpec, spec -> {
      /**
       * This class delayed all the validity check here to avoid unnecessary overhead for every request
       * when application always specify the same compute operations.
       */
      // Check the validity first
      Set<String> computeResultFields = commonValidityCheck();

      countOperations.forEach(
          count -> checkComputeFieldValidity(
              count.field.toString(),
              count.resultFieldName.toString(),
              computeResultFields,
              COUNT));

      // Generate result schema
      List<Schema.Field> resultSchemaFields = getCommonResultFields();
      countOperations.forEach(count -> {
        Schema.Field countField =
            AvroCompatibilityHelper.createSchemaField(count.resultFieldName.toString(), COUNT_RESULT_SCHEMA, "", null);
        resultSchemaFields.add(countField);
      });

      Schema generatedResultSchema = Schema.createRecord(resultSchemaName, "", "", false);
      generatedResultSchema.setFields(resultSchemaFields);
      return Pair.create(generatedResultSchema, generatedResultSchema.toString());
    });
  }

  @Override
  protected ComputeRequestWrapper generateComputeRequest(String resultSchemaStr) {
    // Generate ComputeRequestWrapper object
    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(COMPUTE_REQUEST_VERSION);
    computeRequestWrapper.setResultSchemaStr(resultSchemaStr);
    computeRequestWrapper.setOperations(getComputeRequestOperations());
    computeRequestWrapper.setValueSchema(latestValueSchema);
    return computeRequestWrapper;
  }

  protected List<ComputeOperation> getComputeRequestOperations() {
    List<ComputeOperation> operations = getCommonComputeOperations();

    countOperations.forEach(count -> {
      ComputeOperation computeOperation = new ComputeOperation();
      computeOperation.operationType = COUNT.getValue();
      computeOperation.operation = count;
      operations.add(computeOperation);
    });
    return operations;
  }

  @Override
  public ComputeRequestBuilder<K> count(String inputFieldName, String resultFieldName) {
    Count count = (Count) COUNT.getNewInstance();
    count.field = inputFieldName;
    count.resultFieldName = resultFieldName;
    countOperations.add(count);
    return this;
  }

  @Override
  public void executeWithFilter(Predicate predicate, StreamingCallback<GenericRecord, GenericRecord> callback) {
    throw new VeniceException("ExecuteWithFilter is not supported in V3 compute request.");
  }
}
