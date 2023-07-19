package com.linkedin.venice.compute;

import static com.linkedin.venice.serializer.FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer;
import static com.linkedin.venice.serializer.SerializerDeserializerFactory.getAvroGenericSerializer;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV2;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV3;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV4;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;


/**
 * ComputeRequestWrapper is the formal way of evolving compute request version;
 * the general idea is to keep schemas and request classes for all versions.
 *
 * Compute request will specify its own version in the request header and backend
 * will deserialize the compute request using the corresponding version class and
 * schema.
 */
public class ComputeRequestWrapper {
  public static final int LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST = 3;

  private static final RecordDeserializer[] DESERIALIZER_ARRAY = new RecordDeserializer[] { null,
      getFastAvroSpecificDeserializer(ComputeRequestV1.SCHEMA$, ComputeRequestV1.class),
      getFastAvroSpecificDeserializer(ComputeRequestV2.SCHEMA$, ComputeRequestV2.class),
      getFastAvroSpecificDeserializer(ComputeRequestV3.SCHEMA$, ComputeRequestV3.class),
      getFastAvroSpecificDeserializer(ComputeRequestV4.SCHEMA$, ComputeRequestV4.class) };

  private static final RecordSerializer[] SERIALIZER_ARRAY = new RecordSerializer[] { null,
      getAvroGenericSerializer(ComputeRequestV1.SCHEMA$), getAvroGenericSerializer(ComputeRequestV2.SCHEMA$),
      getAvroGenericSerializer(ComputeRequestV3.SCHEMA$), getAvroGenericSerializer(ComputeRequestV4.SCHEMA$) };

  private int version;
  private Object computeRequest;
  private Schema valueSchema;

  public ComputeRequestWrapper(int version) {
    this.version = version;
    switch (version) {
      case 1:
        computeRequest = new ComputeRequestV1();
        break;
      case 2:
        computeRequest = new ComputeRequestV2();
        break;
      case 3:
        computeRequest = new ComputeRequestV3();
        break;
      case 4:
        computeRequest = new ComputeRequestV4();
        break;
      default:
        throw new VeniceException("Compute request version " + version + " is not support yet.");
    }
  }

  public byte[] serialize() {
    return SERIALIZER_ARRAY[version].serialize(computeRequest);
  }

  public void deserialize(BinaryDecoder decoder) {
    this.computeRequest = DESERIALIZER_ARRAY[version].deserialize(computeRequest, decoder);
  }

  public int getComputeRequestVersion() {
    return version;
  }

  public CharSequence getResultSchemaStr() {
    switch (version) {
      case 1:
        return ((ComputeRequestV1) computeRequest).resultSchemaStr;
      case 2:
        return ((ComputeRequestV2) computeRequest).resultSchemaStr;
      case 3:
        return ((ComputeRequestV3) computeRequest).resultSchemaStr;
      case 4:
        return ((ComputeRequestV4) computeRequest).resultSchemaStr;
      default:
        throw new VeniceException("Compute request version " + version + " is not support yet.");
    }
  }

  public void setValueSchema(Schema schema) {
    this.valueSchema = schema;
  }

  public Schema getValueSchema() {
    return this.valueSchema;
  }

  public void setResultSchemaStr(String resultSchemaStr) {
    switch (version) {
      case 1:
        ((ComputeRequestV1) computeRequest).resultSchemaStr = resultSchemaStr;
        break;
      case 2:
        ((ComputeRequestV2) computeRequest).resultSchemaStr = resultSchemaStr;
        break;
      case 3:
        ((ComputeRequestV3) computeRequest).resultSchemaStr = resultSchemaStr;
        break;
      case 4:
        ((ComputeRequestV4) computeRequest).resultSchemaStr = resultSchemaStr;
        break;
      default:
        throw new VeniceException("Compute request version " + version + " is not support yet.");
    }
  }

  /**
   * Use V2 ComputeOperation for both v1 and v2 request since ComputeOperation V2 is backward compatible
   * with ComputeOperation V1.
   * @return
   */
  public List<ComputeOperation> getOperations() {
    switch (version) {
      case 1:
        return (List) ((ComputeRequestV1) computeRequest).operations;
      case 2:
        return (List) ((ComputeRequestV2) computeRequest).operations;
      case 3:
        return (List) ((ComputeRequestV3) computeRequest).operations;
      case 4:
        return (List) ((ComputeRequestV4) computeRequest).operations;
      default:
        throw new VeniceException("Compute request version " + version + " is not support yet.");
    }
  }

  public void setOperations(List<ComputeOperation> operations) {
    switch (version) {
      case 1:
        ((ComputeRequestV1) computeRequest).operations = (List) operations;
        break;
      case 2:
        ((ComputeRequestV2) computeRequest).operations = (List) operations;
        break;
      case 3:
        ((ComputeRequestV3) computeRequest).operations = (List) operations;
        break;
      case 4:
        ((ComputeRequestV4) computeRequest).operations = (List) operations;
        break;
      default:
        throw new VeniceException("Compute request version " + version + " is not support yet.");
    }
  }
}
