package com.linkedin.venice.compute;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class provides utilities for float-vector operations, and it also handles {@link PrimitiveFloatList}
 * transparently to the user of this class.
 */
public class ComputeOperationUtils {
  public static final String CACHED_SQUARED_L2_NORM_KEY = "CACHED_SQUARED_L2_NORM_KEY";

  public static float dotProduct(List<Float> list1, List<Float> list2) {
    if (list1.size() != list2.size()) {
      throw new VeniceException("Two lists are with different dimensions: " + list1.size() + ", and " + list2.size());
    }
    if (list1 instanceof PrimitiveFloatList && list2 instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList1 = (PrimitiveFloatList) list1;
      PrimitiveFloatList primitiveFloatList2 = (PrimitiveFloatList) list2;
      return dotProduct(list1.size(), primitiveFloatList1::getPrimitive, primitiveFloatList2::getPrimitive);
    } else {
      return dotProduct(list1.size(), list1::get, list2::get);
    }
  }

  public static List<Float> hadamardProduct(List<Float> list1, List<Float> list2) {
    if (list1.size() != list2.size()) {
      throw new VeniceException("Two lists are with different dimensions: " + list1.size() + ", and " + list2.size());
    }
    if (list1 instanceof PrimitiveFloatList && list2 instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList1 = (PrimitiveFloatList) list1;
      PrimitiveFloatList primitiveFloatList2 = (PrimitiveFloatList) list2;
      return hadamardProduct(list1.size(), primitiveFloatList1::getPrimitive, primitiveFloatList2::getPrimitive);
    } else {
      return hadamardProduct(list1.size(), list1::get, list2::get);
    }
  }

  private interface FloatSupplierByIndex {
    float get(int index);
  }

  private static float dotProduct(int size, FloatSupplierByIndex floatSupplier1, FloatSupplierByIndex floatSupplier2) {
    float dotProductResult = 0.0f;

    // round up size to the largest multiple of 4
    int i = 0;
    int limit = (size >> 2) << 2;

    // Unrolling mult-add into blocks of 4 multiply op and assign to 4 different variables so that CPU can take
    // advantage of out of order execution, making the operation faster (on a single thread ~2x improvement)
    for (; i < limit; i += 4) {
      float s0 = floatSupplier1.get(i) * floatSupplier2.get(i);
      float s1 = floatSupplier1.get(i + 1) * floatSupplier2.get(i + 1);
      float s2 = floatSupplier1.get(i + 2) * floatSupplier2.get(i + 2);
      float s3 = floatSupplier1.get(i + 3) * floatSupplier2.get(i + 3);

      dotProductResult += (s0 + s1 + s2 + s3);
    }

    // Multiply the remaining elements
    for (; i < size; i++) {
      dotProductResult += floatSupplier1.get(i) * floatSupplier2.get(i);
    }
    return dotProductResult;
  }

  private static List<Float> hadamardProduct(
      int size,
      FloatSupplierByIndex floatSupplier1,
      FloatSupplierByIndex floatSupplier2) {
    float[] floats = new float[size];

    // round up size to the largest multiple of 4
    int i = 0;
    int limit = (size >> 2) << 2;

    // Unrolling mult-add into blocks of 4 multiply op and assign to 4 different variables so that CPU can take
    // advantage of out of order execution, making the operation faster (on a single thread ~2x improvement)
    for (; i < limit; i += 4) {
      floats[i] = floatSupplier1.get(i) * floatSupplier2.get(i);
      floats[i + 1] = floatSupplier1.get(i + 1) * floatSupplier2.get(i + 1);
      floats[i + 2] = floatSupplier1.get(i + 2) * floatSupplier2.get(i + 2);
      floats[i + 3] = floatSupplier1.get(i + 3) * floatSupplier2.get(i + 3);
    }

    // Multiply the remaining elements
    for (; i < size; i++) {
      floats[i] = floatSupplier1.get(i) * floatSupplier2.get(i);
    }
    return CollectionUtils.asUnmodifiableList(floats);
  }

  public static float squaredL2Norm(List<Float> list) {
    if (list instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList = (PrimitiveFloatList) list;
      int size = primitiveFloatList.size();
      FloatSupplierByIndex floatSupplierByIndex = primitiveFloatList::getPrimitive;
      return dotProduct(size, floatSupplierByIndex, floatSupplierByIndex);
    } else {
      int size = list.size();
      FloatSupplierByIndex floatSupplierByIndex = list::get;
      return dotProduct(size, floatSupplierByIndex, floatSupplierByIndex);
    }
  }

  /**
   *
   * @param record the record from which the value of the given field is extracted
   * @param fieldName name of the file which is used to extract the value from the given record
   * @param <T> Type of the list element to cast the extracted value to
   *
   * @return An unmodifiable empty list if the extracted value is null. Otherwise return a list that may or may not be
   *         modifiable depending on specified type of the list as a field in the record.
   */
  public static <T> List<T> getNullableFieldValueAsList(final GenericRecord record, final String fieldName) {
    Object value = record.get(fieldName);
    if (value == null) {
      return Collections.emptyList();
    }
    if (!(value instanceof List)) {
      throw new IllegalArgumentException(
          String.format("Field %s in the record is not of the type list. Value: %s", fieldName, record));
    }
    return (List<T>) value;
  }

  /**
   * @return Error message if the nullable field validation failed or Optional.empty() otherwise.
   */
  public static Optional<String> validateNullableFieldAndGetErrorMsg(
      ReadComputeOperator operator,
      GenericRecord valueRecord,
      String operatorFieldName) {
    if (valueRecord.get(operatorFieldName) != null) {
      return Optional.empty();
    }
    if (valueRecord.getSchema().getField(operatorFieldName) == null) {
      return Optional.of(
          "Failed to execute compute request as the field " + operatorFieldName
              + " does not exist in the value record. " + "Fields present in the value record are: "
              + getStringOfSchemaFieldNames(valueRecord));
    }

    // Field exist and the value is null. That means the field is nullable.
    if (operator.allowFieldValueToBeNull()) {
      return Optional.empty();
    }
    return Optional.of(
        "Failed to execute compute request as the field " + operatorFieldName + " is not allowed to be null for "
            + operator + " in value record. ");
  }

  private static String getStringOfSchemaFieldNames(GenericRecord valueRecord) {
    List<String> fieldNames =
        valueRecord.getSchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    return String.join(", ", fieldNames);
  }
}
