package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.HeapSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.lang.reflect.Constructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class HeapSizeEstimatorTest {
  private static final Logger LOGGER = LogManager.getLogger(HeapSizeEstimatorTest.class);
  private static final String[] HEADER_ROW = new String[] { "Class name", "predicted", "expected", "allocated" };
  private static final int[] RESULT_ROW_CELL_LENGTHS =
      new int[] { SubSubClassWithThreePrimitiveBooleanFields.class.getSimpleName().length(), HEADER_ROW[1].length(),
          HEADER_ROW[2].length(), HEADER_ROW[3].length() };
  /**
   * Some scenarios are tricky to compute dynamically without just copy-pasting the whole main code, so we just skip it
   * for now, though we could come back to it later...
   */
  private static final int SKIP_EXPECTED_FIELD_OVERHEAD = -1;
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final int NUMBER_OF_ALLOCATIONS_WHEN_MEASURING = 1_000_000;
  private static final int JAVA_MAJOR_VERSION = Utils.getJavaMajorVersion();
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

    LOGGER.info("Java major version: " + JAVA_MAJOR_VERSION);
    LOGGER.info("Alignment size: " + ALIGNMENT_SIZE);
    LOGGER.info("Object header size: " + OBJECT_HEADER_SIZE);
    LOGGER.info("Array header size: " + ARRAY_HEADER_SIZE);
    LOGGER.info("Pointer size: " + POINTER_SIZE);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void test(boolean measureVM) {
    if (measureVM) {
      LOGGER.info(formatResultRow(HEADER_ROW));
    }

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
    if (JAVA_MAJOR_VERSION < 15) {
      // TODO: Plug in correct expected field size for these JVMs...
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveBooleanFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveByteFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveCharFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveShortFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);

      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveBooleanFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveByteFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveCharFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveShortFields.class, SKIP_EXPECTED_FIELD_OVERHEAD);
    } else {
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
      testFieldOverhead(measureVM, SubClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);

      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveBooleanFields.class, BOOLEAN_SIZE * 3);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveByteFields.class, Byte.BYTES * 3);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveCharFields.class, Character.BYTES * 3);
      testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveShortFields.class, Short.BYTES * 3);
    }

    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    testFieldOverhead(measureVM, SubClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveIntFields.class, Integer.BYTES * 3);
    testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveFloatFields.class, Float.BYTES * 3);
    testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveLongFields.class, Long.BYTES * 3);
    testFieldOverhead(measureVM, SubSubClassWithThreePrimitiveDoubleFields.class, Double.BYTES * 3);

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
    int predictedClassOverhead = getClassOverhead(c);
    int expectedClassOverheadWithoutAlignment = OBJECT_HEADER_SIZE + expectedFieldOverhead;
    int expectedClassOverhead;
    if (expectedFieldOverhead == SKIP_EXPECTED_FIELD_OVERHEAD) {
      expectedClassOverhead = SKIP_EXPECTED_FIELD_OVERHEAD;
    } else {
      expectedClassOverhead = roundUpToNearestAlignment(expectedClassOverheadWithoutAlignment);
      assertEquals(predictedClassOverhead, expectedClassOverhead);
    }

    if (measureVM) {
      /**
       * The reason for having multiple attempts is that the memory allocation method is not always reliable.
       * Presumably, this is because GC could kick in during the middle of the allocation loop. If the allocated memory
       * is negative then for sure it's not right. If the GC reduces memory allocated but not enough to make the
       * measurement go negative, then we cannot know if it's a measurement error, or a bug...
       */
      int attempts = 3;
      while (attempts-- > 0) {
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
        if (memoryAllocatedByInstantiations < 0) {
          String errorMessage = "Memory allocated is negative! memoryAllocatedBeforeInstantiations: "
              + memoryAllocatedBeforeInstantiations + ", memoryAllocatedAfterInstantiations: "
              + memoryAllocatedAfterInstantiations + ", memoryAllocatedByInstantiations: "
              + memoryAllocatedByInstantiations + ". " + attempts + " attempts left.";
          if (attempts > 0) {
            LOGGER.info(errorMessage);
            continue;
          } else {
            fail(errorMessage);
          }
        }

        double memoryAllocatedPerInstance =
            (double) memoryAllocatedByInstantiations / (double) NUMBER_OF_ALLOCATIONS_WHEN_MEASURING;

        for (int i = 0; i < NUMBER_OF_ALLOCATIONS_WHEN_MEASURING; i++) {
          assertNotNull(allocations[i]);
        }

        LOGGER.info(
            formatResultRow(
                c.getSimpleName(),
                String.valueOf(predictedClassOverhead),
                String.valueOf(expectedClassOverhead),
                String.format("%.3f", memoryAllocatedPerInstance)));

        assertEquals(
            memoryAllocatedPerInstance,
            predictedClassOverhead,
            0.125, // 1 bit of margin of error... seems to be sufficient for this measurement method
            "The memory allocation measurement is too far from the predictedClassOverhead for class: "
                + c.getSimpleName() + ".");

        // A best-effort attempt to minimize the chance of needing to GC in the middle of the next measurement run...
        allocations = null;
        System.gc();

        break; // No more attempts needed if the allocation measurement and all assertions succeeded
      }
    }
  }

  /** Different algo that the main code because why not? It should be equivalent... */
  private static int roundUpToNearestAlignment(int size) {
    double numberOfAlignmentWindowsFittingWithinTheSize = (double) size / ALIGNMENT_SIZE;
    double roundedUp = Math.ceil(numberOfAlignmentWindowsFittingWithinTheSize);
    int finalSize = (int) roundedUp * ALIGNMENT_SIZE;
    return finalSize;
  }

  private long getCurrentlyAllocatedMemory() {
    System.gc();
    return RUNTIME.maxMemory() - RUNTIME.freeMemory();
  }

  private String formatResultRow(String... cells) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cells.length; i++) {
      sb.append("| ");
      String cell = cells[i];
      int remainder = RESULT_ROW_CELL_LENGTHS[i] - cell.length();

      sb.append(cell);
      for (int j = 0; j < remainder; j++) {
        sb.append(' ');
      }
    }
    sb.append(" |");
    return sb.toString();
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

  private static class SubSubClassWithThreePrimitiveBooleanFields extends SubClassWithTwoPrimitiveBooleanFields {
    private boolean privateField;

    public SubSubClassWithThreePrimitiveBooleanFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveByteFields extends SubClassWithTwoPrimitiveByteFields {
    private byte privateField;

    public SubSubClassWithThreePrimitiveByteFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveCharFields extends SubClassWithTwoPrimitiveCharFields {
    private char privateField;

    public SubSubClassWithThreePrimitiveCharFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveShortFields extends SubClassWithTwoPrimitiveShortFields {
    private short privateField;

    public SubSubClassWithThreePrimitiveShortFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveIntFields extends SubClassWithTwoPrimitiveIntFields {
    private int privateField;

    public SubSubClassWithThreePrimitiveIntFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveFloatFields extends SubClassWithTwoPrimitiveFloatFields {
    private float privateField;

    public SubSubClassWithThreePrimitiveFloatFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveLongFields extends SubClassWithTwoPrimitiveLongFields {
    private long privateField;

    public SubSubClassWithThreePrimitiveLongFields() {
    }
  }

  private static class SubSubClassWithThreePrimitiveDoubleFields extends SubClassWithTwoPrimitiveDoubleFields {
    private double privateField;

    public SubSubClassWithThreePrimitiveDoubleFields() {
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
