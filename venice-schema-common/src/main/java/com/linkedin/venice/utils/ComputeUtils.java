package com.linkedin.venice.utils;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.Count;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;


public class ComputeUtils {
  public static final Pattern VALID_AVRO_NAME_PATTERN = Pattern.compile("\\A[A-Za-z_][A-Za-z0-9_]*\\z");
  public static final String ILLEGAL_AVRO_CHARACTER = "[^A-Za-z0-9_]";
  public static final String ILLEGAL_AVRO_CHARACTER_REPLACEMENT = "_";

  public static void checkResultSchema(Schema resultSchema, Schema valueSchema, int version, List<ComputeOperation> operations) {
    if (resultSchema.getType() != Schema.Type.RECORD || valueSchema.getType() != Schema.Type.RECORD) {
      throw new VeniceException("Compute result schema and value schema must be RECORD type");
    }

    Set<Schema.Field> valueFields = new HashSet<>(valueSchema.getFields());
    Set<String> valueFieldStrings = new HashSet<>();
    valueFields.forEach(field -> valueFieldStrings.add(field.name()));
    Set<Pair<String, Schema.Type>> operationResultFields = new HashSet<>();

    for (ComputeOperation operation : operations) {
      switch (ComputeOperationType.valueOf(operation)) {
        case DOT_PRODUCT:
          DotProduct dotProduct = (DotProduct)operation.operation;
          if (!valueFieldStrings.contains(dotProduct.field.toString())) {
            throw new VeniceException("The field " + dotProduct.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(dotProduct.resultFieldName.toString(), Schema.Type.UNION));
          break;
        case COSINE_SIMILARITY:
          CosineSimilarity cosineSimilarity = (CosineSimilarity)operation.operation;
          if (!valueFieldStrings.contains(cosineSimilarity.field.toString())) {
            throw new VeniceException("The field " + cosineSimilarity.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(cosineSimilarity.resultFieldName.toString(),  Schema.Type.UNION));
          break;
        case HADAMARD_PRODUCT:
          HadamardProduct hadamardProduct = (HadamardProduct)operation.operation;
          if (!valueFieldStrings.contains(hadamardProduct.field.toString())) {
            throw new VeniceException("The field " + hadamardProduct.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(hadamardProduct.resultFieldName.toString(), Schema.Type.UNION));
          break;
        case COUNT:
          Count count = (Count) operation.operation;
          if (!valueFieldStrings.contains(count.field.toString())) {
            throw new VeniceException("The field " + count.field.toString() + " being operated on is not in value schema");
          }
          operationResultFields.add(new Pair<>(count.resultFieldName.toString(), Schema.Type.UNION));
          break;
        default:
          throw new VeniceException("Compute operation type " + operation.operationType + " not supported");
      }
    }
    for (Schema.Field resultField : resultSchema.getFields()) {
      if (valueFields.contains(resultField)) {
        continue;
      }
      if (resultField.name().equals(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)) {
        continue;
      }
      Pair<String, Schema.Type> resultFieldPair = new Pair<>(resultField.name(), resultField.schema().getType());
      if (!operationResultFields.contains(resultFieldPair)) {
        throw new VeniceException("The result field " + resultField.name() +
            " is not a field in value schema or an operation result schema.");
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
    if (null == name) {
      throw new NullPointerException("The name parameter must be specified");
    }
    Matcher m = VALID_AVRO_NAME_PATTERN.matcher(name);
    if (m.matches()) {
      return name;
    }

    return name.replaceAll(ILLEGAL_AVRO_CHARACTER, ILLEGAL_AVRO_CHARACTER_REPLACEMENT);
  }
}
