package com.linkedin.venice.memory;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;


public class ClassSizeEstimatorTest extends HeapSizeEstimatorTest {
  public ClassSizeEstimatorTest() {
    super(SubSubClassWithThreePrimitiveBooleanFields.class);
  }

  @Test
  public void testParamValidation() {
    assertThrows(NullPointerException.class, () -> getClassOverhead(null));
  }

  @Test(dataProvider = "testMethodologies")
  public void testClassOverhead(TestMethodology tm) {
    printHeader(tm.resultsTableHeader);

    TestFunction tf = tm.tfProvider.apply(this);

    // Most basic case... just a plain Object.
    tf.test(Object.class, 0);

    // Ensure that inheritance (in and of itself) adds no overhead.
    tf.test(SubclassOfObjectWithNoFields.class, 0);

    // Ensure that one public primitive fields within a single class is accounted.
    tf.test(ClassWithOnePublicPrimitiveBooleanField.class, BOOLEAN_SIZE);
    tf.test(ClassWithOnePublicPrimitiveByteField.class, Byte.BYTES);
    tf.test(ClassWithOnePublicPrimitiveCharField.class, Character.BYTES);
    tf.test(ClassWithOnePublicPrimitiveShortField.class, Short.BYTES);
    tf.test(ClassWithOnePublicPrimitiveIntField.class, Integer.BYTES);
    tf.test(ClassWithOnePublicPrimitiveFloatField.class, Float.BYTES);
    tf.test(ClassWithOnePublicPrimitiveLongField.class, Long.BYTES);
    tf.test(ClassWithOnePublicPrimitiveDoubleField.class, Double.BYTES);

    // Ensure that two private primitive fields within a single class are accounted.
    tf.test(ClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
    tf.test(ClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    tf.test(ClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    // Ensure that a mix of public and private fields across the class hierarchy are accounted.
    if (JAVA_MAJOR_VERSION < 15) {
      // TODO: Plug in correct expected field size for these JVMs...
      tf.test(SubClassWithTwoPrimitiveBooleanFields.class);
      tf.test(SubClassWithTwoPrimitiveByteFields.class);
      tf.test(SubClassWithTwoPrimitiveCharFields.class);
      tf.test(SubClassWithTwoPrimitiveShortFields.class);

      tf.test(SubSubClassWithThreePrimitiveBooleanFields.class);
      tf.test(SubSubClassWithThreePrimitiveByteFields.class);
      tf.test(SubSubClassWithThreePrimitiveCharFields.class);
      tf.test(SubSubClassWithThreePrimitiveShortFields.class);
    } else {
      tf.test(SubClassWithTwoPrimitiveBooleanFields.class, BOOLEAN_SIZE * 2);
      tf.test(SubClassWithTwoPrimitiveByteFields.class, Byte.BYTES * 2);
      tf.test(SubClassWithTwoPrimitiveCharFields.class, Character.BYTES * 2);
      tf.test(SubClassWithTwoPrimitiveShortFields.class, Short.BYTES * 2);

      tf.test(SubSubClassWithThreePrimitiveBooleanFields.class, BOOLEAN_SIZE * 3);
      tf.test(SubSubClassWithThreePrimitiveByteFields.class, Byte.BYTES * 3);
      tf.test(SubSubClassWithThreePrimitiveCharFields.class, Character.BYTES * 3);
      tf.test(SubSubClassWithThreePrimitiveShortFields.class, Short.BYTES * 3);
    }

    tf.test(SubClassWithTwoPrimitiveIntFields.class, Integer.BYTES * 2);
    tf.test(SubClassWithTwoPrimitiveFloatFields.class, Float.BYTES * 2);
    tf.test(SubClassWithTwoPrimitiveLongFields.class, Long.BYTES * 2);
    tf.test(SubClassWithTwoPrimitiveDoubleFields.class, Double.BYTES * 2);

    tf.test(SubSubClassWithThreePrimitiveIntFields.class, Integer.BYTES * 3);
    tf.test(SubSubClassWithThreePrimitiveFloatFields.class, Float.BYTES * 3);
    tf.test(SubSubClassWithThreePrimitiveLongFields.class, Long.BYTES * 3);
    tf.test(SubSubClassWithThreePrimitiveDoubleFields.class, Double.BYTES * 3);

    // Ensure that pointers are properly accounted.
    tf.test(ClassWithThreeObjectPointers.class, POINTER_SIZE * 3);

    // Ensure that arrays are properly accounted.
    tf.test(ClassWithArray.class, POINTER_SIZE);
    tf.test(Object[].class, () -> new Object[0]);
    tf.test(boolean[].class, () -> new boolean[0]);
    tf.test(byte[].class, () -> new byte[0]);
    tf.test(char[].class, () -> new char[0]);
    tf.test(short[].class, () -> new short[0]);
    tf.test(int[].class, () -> new int[0]);
    tf.test(float[].class, () -> new float[0]);
    tf.test(long[].class, () -> new long[0]);
    tf.test(double[].class, () -> new double[0]);

    /**
     * Ensure that field packing and ordering is accounted.
     *
     * Note that we don't actually do anything special about packing and ordering, and things still work. The ordering
     * can have implications in terms of padding fields to protect against false sharing, but does not appear to have
     * a concrete effect on memory utilization (or at least, none of the current test cases expose such issue).
     */
    tf.test(FieldPacking.class);
    tf.test(FieldOrder.class);

    // Put it all together...
    tf.test(ComplexClass.class, POINTER_SIZE * 10);

    printResultSeparatorLine();
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
    Object field1, field2, field3;

    public ClassWithThreeObjectPointers() {
    }
  }

  private static class ClassWithArray {
    public Object[] array;

    public ClassWithArray() {
    }
  }

  /** See: https://shipilev.net/jvm/objects-inside-out/#_field_packing */
  private static class FieldPacking {
    boolean b;
    long l;
    char c;
    int i;

    public FieldPacking() {
    }
  }

  /** See: https://shipilev.net/jvm/objects-inside-out/#_observation_field_declaration_order_field_layout_order */
  private static class FieldOrder {
    boolean firstField;
    long secondField;
    char thirdField;
    int fourthField;

    public FieldOrder() {
    }
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

    public ComplexClass() {
    }
  }
}
