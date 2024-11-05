package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.HeapSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.utils.DataProviderUtils;
import java.lang.reflect.Constructor;
import org.testng.annotations.Test;


public class HeapSizeEstimatorTest {
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final int NUMBER_OF_ALLOCATIONS_WHEN_MEASURING = 1_000_000;
  private static final int BOOLEAN_SIZE = 1;
  private static final int ALIGNMENT_SIZE;
  private static final int OBJECT_HEADER_SIZE;
  private static final int ARRAY_HEADER_SIZE;
  private static final int POINTER_SIZE;

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

    System.out.println("64 bits JVM: " + is64bitsJVM);
    System.out.println("Alignment size: " + ALIGNMENT_SIZE);
    System.out.println("Object header size: " + OBJECT_HEADER_SIZE);
    System.out.println("Array header size: " + ARRAY_HEADER_SIZE);
    System.out.println("Pointer size: " + POINTER_SIZE);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void test(boolean measureVM) {
    // Most basic case... just a plain Object.
    testFieldOverhead(measureVM, Object.class, 0);

    // Ensure that inheritance (in and of itself) adds no overhead.
    testFieldOverhead(measureVM, SubclassOfObjectWithNoFields.class, 0);

    // Ensure that one public primitive fields within a single class is accounted.
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveBooleanField.class, BOOLEAN_SIZE);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveByteField.class, Byte.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveCharField.class, Character.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveShortField.class, Short.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveIntField.class, Integer.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveFloatField.class, Float.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveLongField.class, Long.BYTES);
    testFieldOverhead(measureVM, ClassWithOnePublicPrimitiveDoubleField.class, Double.BYTES);

    // Ensure that two private primitive fields within a single class are accounted.
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    testFieldOverhead(measureVM, ClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    // Ensure that a mix of public and private fields across the class hierarchy are accounted.
    // TODO: Fixt the small fields (< 4 bytes), which don't work as expected due to layout issues
    // testFieldOverhead(measureVM, SubClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
    // testFieldOverhead(measureVM, SubClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
    // testFieldOverhead(measureVM, SubClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
    // testFieldOverhead(measureVM, SubClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    // Ensure that pointers are properly accounted.
    int classWithThreeObjectPointersFieldOverhead = (POINTER_SIZE + roundUpToNearestAlignment(OBJECT_HEADER_SIZE)) * 3;
    testFieldOverhead(measureVM, ClassWithThreeObjectPointers.class, classWithThreeObjectPointersFieldOverhead);

    // Ensure that arrays are properly accounted.
    int classWithArrayFieldOverhead = POINTER_SIZE + ARRAY_HEADER_SIZE;
    testFieldOverhead(measureVM, ClassWithArray.class, classWithArrayFieldOverhead);

    // Put it all together...
    testFieldOverhead(
        measureVM,
        ComplexClass.class,
        POINTER_SIZE * 10 + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + BOOLEAN_SIZE * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Byte.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Character.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Short.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Integer.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Float.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Long.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + Double.BYTES * 2)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + classWithThreeObjectPointersFieldOverhead)
            + roundUpToNearestAlignment(OBJECT_HEADER_SIZE + classWithArrayFieldOverhead));
  }

  private void testFieldOverhead(boolean measureVM, Class c, int expectedFieldOverhead) {
    int actualClassOverhead = getClassOverhead(c);
    int expectedClassOverheadWithoutAlignment = OBJECT_HEADER_SIZE + expectedFieldOverhead;
    int expectedClassOverhead = roundUpToNearestAlignment(expectedClassOverheadWithoutAlignment);
    assertEquals(actualClassOverhead, expectedClassOverhead);

    if (measureVM) {
      assertNotEquals(RUNTIME.maxMemory(), Long.MAX_VALUE);
      Object[] allocations = new Object[NUMBER_OF_ALLOCATIONS_WHEN_MEASURING];
      Class<?>[] argTypes = new Class[0];
      Object[] args = new Object[0];
      Constructor<?> cons;
      try {
        cons = c.getConstructor(argTypes);
      } catch (NoSuchMethodException e) {
        fail("Could not get a no-arg constructor for " + c.getSimpleName(), e);
        throw new RuntimeException(e);
      }

      long memoryAllocatedBeforeInstantiations = getCurrentlyAllocatedMemory();

      try {
        for (int i = 0; i < NUMBER_OF_ALLOCATIONS_WHEN_MEASURING; i++) {
          allocations[i] = cons.newInstance(args);
        }
      } catch (Exception e) {
        fail("Could not invoke the no-arg constructor for " + c.getSimpleName(), e);
      }

      long memoryAllocatedAfterInstantiations = getCurrentlyAllocatedMemory();
      long memoryAllocatedByInstantiations = memoryAllocatedAfterInstantiations - memoryAllocatedBeforeInstantiations;
      assertTrue(memoryAllocatedByInstantiations > 0, "No memory allocated?!");
      double memoryAllocatedPerInstance =
          (double) memoryAllocatedByInstantiations / (double) NUMBER_OF_ALLOCATIONS_WHEN_MEASURING;

      for (int i = 0; i < NUMBER_OF_ALLOCATIONS_WHEN_MEASURING; i++) {
        assertNotNull(allocations[i]);
      }

      assertEquals(
          memoryAllocatedPerInstance,
          expectedClassOverhead,
          1.0,
          "The memory allocation measurement is too far from the expected value for class: " + c.getSimpleName() + ".");
    }
  }

  /** Different algo that the main code because why not? It should be equivalent... */
  private static int roundUpToNearestAlignment(int size) {
    double numberOfAlignmentWindowsFittingWithinTheSize = (double) size / ALIGNMENT_SIZE;
    double roundedUp = Math.ceil(numberOfAlignmentWindowsFittingWithinTheSize);
    int finalSize = (int) roundedUp * ALIGNMENT_SIZE;
    return finalSize;
  }

  private static long getCurrentlyAllocatedMemory() {
    System.gc();
    return RUNTIME.maxMemory() - RUNTIME.freeMemory();
  }

  private static class SubclassOfObjectWithNoFields {
    public SubclassOfObjectWithNoFields() {
    }
  }

  private static class ClassWithOnePublicPrimitiveBooleanField {
    public boolean publicField;

    public ClassWithOnePublicPrimitiveBooleanField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveByteField {
    public byte publicField;

    public ClassWithOnePublicPrimitiveByteField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveCharField {
    public char publicField;

    public ClassWithOnePublicPrimitiveCharField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveShortField {
    public short publicField;

    public ClassWithOnePublicPrimitiveShortField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveIntField {
    public int publicField;

    public ClassWithOnePublicPrimitiveIntField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveFloatField {
    public float publicField;

    public ClassWithOnePublicPrimitiveFloatField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveLongField {
    public long publicField;

    public ClassWithOnePublicPrimitiveLongField() {
    }
  }

  private static class ClassWithOnePublicPrimitiveDoubleField {
    public double publicField;

    public ClassWithOnePublicPrimitiveDoubleField() {
    }
  }

  private static class ClassWithTwoPrimitiveBooleanFields {
    private boolean field1, field2;

    public ClassWithTwoPrimitiveBooleanFields() {
    }
  }

  private static class ClassWithTwoPrimitiveByteFields {
    private byte field1, field2;

    public ClassWithTwoPrimitiveByteFields() {
    }
  }

  private static class ClassWithTwoPrimitiveCharFields {
    private char field1, field2;

    public ClassWithTwoPrimitiveCharFields() {
    }
  }

  private static class ClassWithTwoPrimitiveShortFields {
    private short field1, field2;

    public ClassWithTwoPrimitiveShortFields() {
    }
  }

  private static class ClassWithTwoPrimitiveIntFields {
    private int field1, field2;

    public ClassWithTwoPrimitiveIntFields() {
    }
  }

  private static class ClassWithTwoPrimitiveFloatFields {
    private float field1, field2;

    public ClassWithTwoPrimitiveFloatFields() {
    }
  }

  private static class ClassWithTwoPrimitiveLongFields {
    private long field1, field2;

    public ClassWithTwoPrimitiveLongFields() {
    }
  }

  private static class ClassWithTwoPrimitiveDoubleFields {
    private double field1, field2;

    public ClassWithTwoPrimitiveDoubleFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveBooleanFields extends ClassWithOnePublicPrimitiveBooleanField {
    private boolean privateField;

    public SubClassWithTwoPrimitiveBooleanFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveByteFields extends ClassWithOnePublicPrimitiveByteField {
    private byte privateField;

    public SubClassWithTwoPrimitiveByteFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveCharFields extends ClassWithOnePublicPrimitiveCharField {
    private char privateField;

    public SubClassWithTwoPrimitiveCharFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveShortFields extends ClassWithOnePublicPrimitiveShortField {
    private short privateField;

    public SubClassWithTwoPrimitiveShortFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveIntFields extends ClassWithOnePublicPrimitiveIntField {
    private int privateField;

    public SubClassWithTwoPrimitiveIntFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveFloatFields extends ClassWithOnePublicPrimitiveFloatField {
    private float privateField;

    public SubClassWithTwoPrimitiveFloatFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveLongFields extends ClassWithOnePublicPrimitiveLongField {
    private long privateField;

    public SubClassWithTwoPrimitiveLongFields() {
    }
  }

  private static class SubClassWithTwoPrimitiveDoubleFields extends ClassWithOnePublicPrimitiveDoubleField {
    private double privateField;

    public SubClassWithTwoPrimitiveDoubleFields() {
    }
  }

  private static class ClassWithThreeObjectPointers {
    Object field1 = new Object();
    Object field2 = new Object();
    Object field3 = new Object();

    public ClassWithThreeObjectPointers() {
    }
  }

  private static class ClassWithArray {
    public Object[] array = new Object[0];

    public ClassWithArray() {
    }
  }

  private static class ComplexClass {
    ClassWithTwoPrimitiveBooleanFields field1 = new ClassWithTwoPrimitiveBooleanFields();
    ClassWithTwoPrimitiveByteFields field2 = new ClassWithTwoPrimitiveByteFields();
    ClassWithTwoPrimitiveCharFields field3 = new ClassWithTwoPrimitiveCharFields();
    ClassWithTwoPrimitiveShortFields field4 = new ClassWithTwoPrimitiveShortFields();
    ClassWithTwoPrimitiveIntFields field5 = new ClassWithTwoPrimitiveIntFields();
    ClassWithTwoPrimitiveFloatFields field6 = new ClassWithTwoPrimitiveFloatFields();
    ClassWithTwoPrimitiveLongFields field7 = new ClassWithTwoPrimitiveLongFields();
    ClassWithTwoPrimitiveDoubleFields field8 = new ClassWithTwoPrimitiveDoubleFields();
    ClassWithThreeObjectPointers field9 = new ClassWithThreeObjectPointers();
    ClassWithArray field10 = new ClassWithArray();

    public ComplexClass() {
    }
  }
}
