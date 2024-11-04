package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.HeapSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.testng.annotations.Test;


public class HeapSizeEstimatorTest {
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  private static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;
  private static final int BOOLEAN_SIZE;

  static {
    // This duplicates the main code, which is not ideal, but there isn't much choice if we want the test to run in
    // various JVM scenarios...
    boolean is64bitsJVM = HeapSizeEstimator.is64bitsJVM();
    int markWordSize = is64bitsJVM ? 8 : 4;
    boolean isCompressedOopsEnabled = HeapSizeEstimator.isUseCompressedOopsEnabled();
    boolean isCompressedKlassPointersEnabled = HeapSizeEstimator.isCompressedKlassPointersEnabled();
    int classPointerSize = isCompressedKlassPointersEnabled ? 4 : 8;

    ALIGNMENT_SIZE = is64bitsJVM ? 8 : 4;
    OBJECT_HEADER_SIZE = markWordSize + classPointerSize;
    ARRAY_HEADER_SIZE = roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES);
    POINTER_SIZE = isCompressedOopsEnabled ? 4 : 8;
    BOOLEAN_SIZE = 1;
  }

  @Test
  public void test() {
    // Most basic case... just a plain Object.
    int objectClassOverhead = getClassOverhead(Object.class);
    assertEquals(objectClassOverhead, OBJECT_HEADER_SIZE);

    // Ensure that inheritance (in and of itself) adds no overhead.
    int subclassOfObjectWithNoFieldsOverhead = getClassOverhead(SubclassOfObjectWithNoFields.class);
    assertEquals(subclassOfObjectWithNoFieldsOverhead, objectClassOverhead);

    // Ensure that public primitive fields are accounted.
    testFieldOverhead(ClassWithOnePublicPrimitiveBooleanField.class, BOOLEAN_SIZE);
    testFieldOverhead(ClassWithOnePublicPrimitiveByteField.class, Byte.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveCharField.class, Character.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveShortField.class, Short.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveIntField.class, Integer.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveFloatField.class, Float.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveLongField.class, Long.BYTES);
    testFieldOverhead(ClassWithOnePublicPrimitiveDoubleField.class, Double.BYTES);

    // Ensure that a mix of public and private fields across the class hierarchy are accounted.
    testFieldOverhead(ClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
    testFieldOverhead(ClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    testFieldOverhead(ClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    // Ensure that pointers are properly accounted.
    testFieldOverhead(ClassWithThreeObjectPointers.class, (POINTER_SIZE + OBJECT_HEADER_SIZE) * 3);

    // Ensure that arrays are properly accounted.
    testFieldOverhead(ClassWithArray.class, POINTER_SIZE + ARRAY_HEADER_SIZE);

    // Put it all together...
    testFieldOverhead(
        ComplexClass.class,
        (POINTER_SIZE) * 10 + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + BOOLEAN_SIZE * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Byte.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Character.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Short.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Float.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Long.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Double.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + (POINTER_SIZE + OBJECT_HEADER_SIZE) * 3)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + POINTER_SIZE + ARRAY_HEADER_SIZE));

    // Test some standard Java library classes
    testFieldOverhead(ArrayList.class, POINTER_SIZE + ARRAY_HEADER_SIZE + Integer.BYTES * 2);
    int expectedHashMapFieldSize =
        POINTER_SIZE * 4 + OBJECT_HEADER_SIZE * 3 + ARRAY_HEADER_SIZE + Integer.BYTES * 3 + Float.BYTES;
    testFieldOverhead(HashMap.class, expectedHashMapFieldSize);
    testFieldOverhead(
        HashSet.class,
        POINTER_SIZE + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + expectedHashMapFieldSize));
  }

  private void testFieldOverhead(Class c, int expectedFieldOverhead) {
    int classOverhead = getClassOverhead(c);
    int classOverheadWithoutAlignment = OBJECT_HEADER_SIZE + expectedFieldOverhead;
    int fieldOverheadWithAlignment = roundUpToNearestAlignment(classOverheadWithoutAlignment);
    assertEquals(classOverhead, fieldOverheadWithAlignment);
  }

  /** Different algo that the main code because why not? It should be equivalent... */
  private static int roundUpToNearestAlignment(int size) {
    double numberOfAlignmentWindowsFittingWithinTheSize = (double) size / ALIGNMENT_SIZE;
    double roundedUp = Math.ceil(numberOfAlignmentWindowsFittingWithinTheSize);
    int finalSize = (int) roundedUp * ALIGNMENT_SIZE;
    return finalSize;
  }

  private static class SubclassOfObjectWithNoFields {
  }

  private static class ClassWithOnePublicPrimitiveBooleanField {
    public boolean publicField;
  }

  private static class ClassWithOnePublicPrimitiveByteField {
    public byte publicField;
  }

  private static class ClassWithOnePublicPrimitiveCharField {
    public char publicField;
  }

  private static class ClassWithOnePublicPrimitiveShortField {
    public short publicField;
  }

  private static class ClassWithOnePublicPrimitiveIntField {
    public int publicField;
  }

  private static class ClassWithOnePublicPrimitiveFloatField {
    public float publicField;
  }

  private static class ClassWithOnePublicPrimitiveLongField {
    public long publicField;
  }

  private static class ClassWithOnePublicPrimitiveDoubleField {
    public double publicField;
  }

  private static class ClassWithTwoPrimitiveBooleanFields extends ClassWithOnePublicPrimitiveBooleanField {
    private boolean privateField;
  }

  private static class ClassWithTwoPrimitiveByteFields extends ClassWithOnePublicPrimitiveByteField {
    private byte privateField;
  }

  private static class ClassWithTwoPrimitiveCharFields extends ClassWithOnePublicPrimitiveCharField {
    private char privateField;
  }

  private static class ClassWithTwoPrimitiveShortFields extends ClassWithOnePublicPrimitiveShortField {
    private short privateField;
  }

  private static class ClassWithTwoPrimitiveIntFields extends ClassWithOnePublicPrimitiveIntField {
    private int privateField;
  }

  private static class ClassWithTwoPrimitiveFloatFields extends ClassWithOnePublicPrimitiveFloatField {
    private float privateField;
  }

  private static class ClassWithTwoPrimitiveLongFields extends ClassWithOnePublicPrimitiveLongField {
    private long privateField;
  }

  private static class ClassWithTwoPrimitiveDoubleFields extends ClassWithOnePublicPrimitiveDoubleField {
    private double privateField;
  }

  private static class ClassWithThreeObjectPointers {
    public Object field1, field2, field3;
  }

  private static class ClassWithArray {
    public Object[] array;
  }

  private static class ComplexClass {
    ClassWithTwoPrimitiveBooleanFields field1;
    ClassWithTwoPrimitiveByteFields field2;
    ClassWithTwoPrimitiveCharFields field3;
    ClassWithTwoPrimitiveShortFields field4;
    ClassWithTwoPrimitiveIntFields field5;
    ClassWithTwoPrimitiveFloatFields field6;
    ClassWithTwoPrimitiveLongFields field7;
    ClassWithTwoPrimitiveDoubleFields field8;
    ClassWithThreeObjectPointers field9;
    ClassWithArray field10;
  }
}
