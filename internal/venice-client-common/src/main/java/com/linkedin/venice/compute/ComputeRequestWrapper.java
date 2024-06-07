package com.linkedin.venice.compute;

import static com.linkedin.venice.serializer.FastSerializerDeserializerFactory.getFastAvroGenericSerializer;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV3;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.List;
import org.apache.avro.Schema;


/**
 * This class is used by the client to encapsulate the information it needs about a compute request.
 *
 * N.B.: This class used to contain multiple versions of the {@link ComputeRequestV3} but it was not necessary
 * since all the versions were anyway compatible with one another. We are now keeping only the latest version
 * used on the wire, which is 3 (version 4 was never used as a wire protocol). We can always revisit this if
 * the need to evolve read compute comes into play.
 */
public class ComputeRequestWrapper {
  public static final int LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST = 3;

  private static final RecordSerializer<ComputeRequestV3> SERIALIZER =
      getFastAvroGenericSerializer(ComputeRequestV3.SCHEMA$);

  private final ComputeRequestV3 computeRequest;
  private final int valueSchemaId;
  private final Schema valueSchema;
  private final List<Schema.Field> operationResultFields;
  private final boolean originallyStreaming;

  public ComputeRequestWrapper(
      int valueSchemaId,
      Schema valueSchema,
      Schema resultSchema,
      String resultSchemaString,
      List<ComputeOperation> operations,
      boolean originallyStreaming) {
    this.computeRequest = new ComputeRequestV3();
    this.computeRequest.setResultSchemaStr(resultSchemaString);
    this.computeRequest.setOperations((List) operations);
    this.valueSchemaId = valueSchemaId;
    this.valueSchema = valueSchema;
    this.operationResultFields = ComputeUtils.getOperationResultFields(operations, resultSchema);
    this.originallyStreaming = originallyStreaming;
  }

  public byte[] serialize() {
    return SERIALIZER.serialize(this.computeRequest);
  }

  public CharSequence getResultSchemaStr() {
    return this.computeRequest.getResultSchemaStr();
  }

  public int getValueSchemaID() {
    return this.valueSchemaId;
  }

  public Schema getValueSchema() {
    return this.valueSchema;
  }

  public List<ComputeOperation> getOperations() {
    return (List) this.computeRequest.getOperations();
  }

  public List<Schema.Field> getOperationResultFields() {
    return this.operationResultFields;
  }

  public boolean isRequestOriginallyStreaming() {
    return this.originallyStreaming;
  }
}
