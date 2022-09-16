package com.linkedin.venice.utils;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.Count;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ComputeUtils {
  private static final Logger LOGGER = LogManager.getLogger(ComputeUtils.class);
  public static final Pattern VALID_AVRO_NAME_PATTERN = Pattern.compile("\\A[A-Za-z_][A-Za-z0-9_]*\\z");
  public static final String ILLEGAL_AVRO_CHARACTER = "[^A-Za-z0-9_]";
  public static final String ILLEGAL_AVRO_CHARACTER_REPLACEMENT = "_";
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public static void checkResultSchema(
      Schema resultSchema,
      Schema valueSchema,
      int version,
      List<ComputeOperation> operations) {
    if (resultSchema.getType() != Schema.Type.RECORD || valueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceException("Compute result schema and value schema must be RECORD type");
    }

    final Map<String, Schema> valueFieldSchemaMap = new HashMap<>(valueSchema.getFields().size());
    valueSchema.getFields().forEach(f -> valueFieldSchemaMap.put(f.name(), f.schema()));
    Set<Pair<String, Schema.Type>> operationResultFields = new HashSet<>();

    for (ComputeOperation operation: operations) {
      switch (ComputeOperationType.valueOf(operation)) {
        case DOT_PRODUCT:
          DotProduct dotProduct = (DotProduct) operation.operation;
          if (!valueFieldSchemaMap.containsKey(dotProduct.field.toString())) {
            throw new VeniceException(
                "The field " + dotProduct.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(dotProduct.resultFieldName.toString(), Schema.Type.UNION));
          break;
        case COSINE_SIMILARITY:
          CosineSimilarity cosineSimilarity = (CosineSimilarity) operation.operation;
          if (!valueFieldSchemaMap.containsKey(cosineSimilarity.field.toString())) {
            throw new VeniceException(
                "The field " + cosineSimilarity.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(cosineSimilarity.resultFieldName.toString(), Schema.Type.UNION));
          break;
        case HADAMARD_PRODUCT:
          HadamardProduct hadamardProduct = (HadamardProduct) operation.operation;
          if (!valueFieldSchemaMap.containsKey(hadamardProduct.field.toString())) {
            throw new VeniceException(
                "The field " + hadamardProduct.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(hadamardProduct.resultFieldName.toString(), Schema.Type.UNION));
          break;
        case COUNT:
          Count count = (Count) operation.operation;
          if (!valueFieldSchemaMap.containsKey(count.field.toString())) {
            throw new VeniceException(
                "The field " + count.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(count.resultFieldName.toString(), Schema.Type.UNION));
          break;
        default:
          throw new VeniceException("Compute operation type " + operation.operationType + " not supported");
      }
    }
    for (Schema.Field resultField: resultSchema.getFields()) {
      /**
       * There is no need to compare whether the 'resultField' is exactly same as the corresponding one in the value schema,
       * since there is no need to make the result schema backward compatible.
       * As long as the schema of the same field is same between the result schema and the value schema, it will be
       * good enough.
       * The major reason we couldn't make sure the same field is exactly same between the result schema and value schema
       * is that it is not easy to achieve on Client side since the way to extract the default value from
       * an existing field changes with Avro-1.9 or above.
       */
      if (resultField.schema().equals(valueFieldSchemaMap.get(resultField.name()))) {
        continue;
      }
      if (resultField.name().equals(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)) {
        continue;
      }
      Pair<String, Schema.Type> resultFieldPair = new Pair<>(resultField.name(), resultField.schema().getType());
      if (!operationResultFields.contains(resultFieldPair)) {
        String msg = "The result field " + resultField.name() + " with schema " + resultField.schema()
            + " for value schema code " + valueSchema.hashCode();
        if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(msg)) {
          LOGGER.error(
              msg + " is not a field in value schema or an operation result schema." + " Value schema " + valueSchema);
        }
        throw new VeniceException(
            "The result field " + resultField.name()
                + " is not a field in value schema or an operation result schema.");
      }
    }
  }

  /**
   * According to Avro specification (https://avro.apache.org/docs/1.7.7/spec.html#Names):
   *
   * The name portion of a fullname, record field names, and enum symbols must:
   *     1. start with [A-Za-z_]
   *     2. subsequently contain only [A-Za-z0-9_]
   *
   * Remove all Avro illegal characters.
   *
   * @param name
   * @return a string that doesn't contain any illegal character
   */
  public static String removeAvroIllegalCharacter(String name) {
    if (name == null) {
      throw new NullPointerException("The name parameter must be specified");
    }
    Matcher m = VALID_AVRO_NAME_PATTERN.matcher(name);
    if (m.matches()) {
      return name;
    }

    return name.replaceAll(ILLEGAL_AVRO_CHARACTER, ILLEGAL_AVRO_CHARACTER_REPLACEMENT);
  }
}
