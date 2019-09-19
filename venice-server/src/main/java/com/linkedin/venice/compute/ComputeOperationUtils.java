package com.linkedin.venice.compute;

import com.linkedin.avro.fastserde.PrimitiveFloatList;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.ComputablePrimitiveFloatList;
import java.util.List;


/**
 * This class provides utilities for float-vector operations, and it also handles {@link ComputablePrimitiveFloatList}
 * and {@link PrimitiveFloatList} transparently to the user of this class.
 */
public class ComputeOperationUtils {
  public static final String CACHED_SQUARED_L2_NORM_KEY = "CACHED_SQUARED_L2_NORM_KEY";

  public static float dotProduct(List<Float> list1, List<Float> list2) {
    if (list1.size() != list2.size()) {
      throw new VeniceException("Two lists are with different dimensions: " + list1.size() + ", and " + list2.size());
    }
    // TODO: Clean up and retire ComputablePrimitiveFloatList after Fast-Avro is adopted by default
    if (list1 instanceof ComputablePrimitiveFloatList && list2 instanceof ComputablePrimitiveFloatList) {
      ComputablePrimitiveFloatList computablePrimitiveFloatList1 = (ComputablePrimitiveFloatList)list1;
      ComputablePrimitiveFloatList computablePrimitiveFloatList2 = (ComputablePrimitiveFloatList)list2;
      return dotProduct(list1.size(), (i) -> computablePrimitiveFloatList1.getPrimitive(i), (i) -> computablePrimitiveFloatList2.getPrimitive(i));
    } else if (list1 instanceof PrimitiveFloatList && list2 instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList1 = (PrimitiveFloatList)list1;
      PrimitiveFloatList primitiveFloatList2 = (PrimitiveFloatList)list2;
      return dotProduct(list1.size(), (i) -> primitiveFloatList1.getPrimitive(i), (i) -> primitiveFloatList2.getPrimitive(i));
    } else {
      return dotProduct(list1.size(), (i) -> list1.get(i), (i) -> list2.get(i));
    }
  }

  public static List<Float> hadamardProduct(List<Float> list1, List<Float> list2) {
    if (list1.size() != list2.size()) {
      throw new VeniceException("Two lists are with different dimensions: " + list1.size() + ", and " + list2.size());
    }
    // TODO: Clean up and retire ComputablePrimitiveFloatList after Fast-Avro is adopted by default
    if (list1 instanceof ComputablePrimitiveFloatList && list2 instanceof ComputablePrimitiveFloatList) {
      ComputablePrimitiveFloatList computablePrimitiveFloatList1 = (ComputablePrimitiveFloatList)list1;
      ComputablePrimitiveFloatList computablePrimitiveFloatList2 = (ComputablePrimitiveFloatList)list2;
      return hadamardProduct(list1.size(), (i) -> computablePrimitiveFloatList1.getPrimitive(i), (i) -> computablePrimitiveFloatList2.getPrimitive(i));
    } else if (list1 instanceof PrimitiveFloatList && list2 instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList1 = (PrimitiveFloatList)list1;
      PrimitiveFloatList primitiveFloatList2 = (PrimitiveFloatList)list2;
      return hadamardProduct(list1.size(), (i) -> primitiveFloatList1.getPrimitive(i), (i) -> primitiveFloatList2.getPrimitive(i));
    } else {
      return hadamardProduct(list1.size(), (i) -> list1.get(i), (i) -> list2.get(i));
    }
  }

  private interface FloatSupplierByIndex {
    float get(int index);
  }

  private static float dotProduct(int size, FloatSupplierByIndex floatSupplier1, FloatSupplierByIndex floatSupplier2) {
    float dotProductResult = 0.0f;

    // round up size to the largest multiple of 4
    int i = 0;
    int limit  = (size >> 2) << 2;

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

  private static List<Float> hadamardProduct(int size, FloatSupplierByIndex floatSupplier1, FloatSupplierByIndex floatSupplier2) {
    PrimitiveFloatList hadamardProductResult = new PrimitiveFloatList(size);

    // round up size to the largest multiple of 4
    int i = 0;
    int limit  = (size >> 2) << 2;

    // Unrolling mult-add into blocks of 4 multiply op and assign to 4 different variables so that CPU can take
    // advantage of out of order execution, making the operation faster (on a single thread ~2x improvement)
    for (; i < limit; i += 4) {
      float s0 = floatSupplier1.get(i) * floatSupplier2.get(i);
      float s1 = floatSupplier1.get(i + 1) * floatSupplier2.get(i + 1);
      float s2 = floatSupplier1.get(i + 2) * floatSupplier2.get(i + 2);
      float s3 = floatSupplier1.get(i + 3) * floatSupplier2.get(i + 3);
      hadamardProductResult.addPrimitive(s0);
      hadamardProductResult.addPrimitive(s1);
      hadamardProductResult.addPrimitive(s2);
      hadamardProductResult.addPrimitive(s3);
    }

    // Multiply the remaining elements
    for (; i < size; i++) {
      hadamardProductResult.addPrimitive(floatSupplier1.get(i) * floatSupplier2.get(i));
    }
    return hadamardProductResult;
  }

  public static float squaredL2Norm(List<Float> list) {
    if (list instanceof ComputablePrimitiveFloatList) {
      return ((ComputablePrimitiveFloatList) list).squaredL2Norm();
    } else if (list instanceof PrimitiveFloatList) {
      PrimitiveFloatList primitiveFloatList = (PrimitiveFloatList)list;
      int size = primitiveFloatList.size();
      FloatSupplierByIndex floatSupplierByIndex = (i) -> primitiveFloatList.getPrimitive(i);
      return dotProduct(size, floatSupplierByIndex, floatSupplierByIndex);
    } else {
      int size = list.size();
      FloatSupplierByIndex floatSupplierByIndex = (i) -> list.get(i);
      return dotProduct(size, floatSupplierByIndex, floatSupplierByIndex);
    }
  }
}
