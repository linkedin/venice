package com.linkedin.venice.utils;

import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.ComparisonPredicate;
import com.linkedin.venice.protocols.LogicalPredicate;
import java.util.Arrays;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PredicateEvaluatorTest {
  @Test
  public void testStringEqualsPredicate() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("test_value").build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, "test_value"));
    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, "other_value"));
  }

  @Test
  public void testStringInPredicate() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder()
                .setOperator("IN")
                .setFieldType("STRING")
                .setValue("value1,value2,value3")
                .build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, "value1"));
    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, "value2"));
    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, "value3"));
    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, "value4"));
  }

  @Test
  public void testIntegerComparisonPredicate() {
    BucketPredicate gtPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("GT").setFieldType("INT").setValue("10").build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(gtPredicate, 15));
    Assert.assertFalse(PredicateEvaluator.evaluate(gtPredicate, 5));
    Assert.assertFalse(PredicateEvaluator.evaluate(gtPredicate, 10));
  }

  @Test
  public void testLogicalAndPredicate() {
    // Create two comparison predicates
    BucketPredicate gtPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("GT").setFieldType("INT").setValue("5").build())
        .build();

    BucketPredicate ltPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("LT").setFieldType("INT").setValue("15").build())
        .build();

    // Create AND logical predicate
    BucketPredicate andPredicate = BucketPredicate.newBuilder()
        .setLogical(
            LogicalPredicate.newBuilder()
                .setOperator("AND")
                .addAllPredicates(Arrays.asList(gtPredicate, ltPredicate))
                .build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(andPredicate, 10)); // 5 < 10 < 15
    Assert.assertFalse(PredicateEvaluator.evaluate(andPredicate, 3)); // 3 <= 5
    Assert.assertFalse(PredicateEvaluator.evaluate(andPredicate, 20)); // 20 >= 15
  }

  @Test
  public void testLogicalOrPredicate() {
    BucketPredicate eqPredicate1 = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("test_name_1").build())
        .build();

    BucketPredicate eqPredicate2 = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("test_name_2").build())
        .build();

    // Create OR logical predicate
    BucketPredicate orPredicate = BucketPredicate.newBuilder()
        .setLogical(
            LogicalPredicate.newBuilder()
                .setOperator("OR")
                .addAllPredicates(Arrays.asList(eqPredicate1, eqPredicate2))
                .build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(orPredicate, "test_name_1"));
    Assert.assertTrue(PredicateEvaluator.evaluate(orPredicate, "test_name_2"));
    Assert.assertFalse(PredicateEvaluator.evaluate(orPredicate, "test_name_3"));
  }

  @Test
  public void testFloatInPredicate() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("IN").setFieldType("FLOAT").setValue("1.5,2.5,3.5").build())
        .build();

    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, 1.5f));
    Assert.assertTrue(PredicateEvaluator.evaluate(predicate, 2.5f));
    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, 4.5f));
  }

  @Test
  public void testNullValues() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("STRING").setValue("test").build())
        .build();

    Assert.assertFalse(PredicateEvaluator.evaluate(null, "test"));
    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, null));
  }

  @Test
  public void testAllIntegerOperators() {
    // Test all operators for INT type
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("GT", "10"), 15));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("GTE", "10"), 10));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("GTE", "10"), 15));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("LT", "10"), 5));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("LTE", "10"), 10));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("LTE", "10"), 5));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), 10));
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("IN", "10"), 10));

    // Test false cases
    Assert.assertFalse(PredicateEvaluator.evaluate(createIntPredicate("GT", "10"), 10));
    Assert.assertFalse(PredicateEvaluator.evaluate(createIntPredicate("LT", "10"), 10));
    Assert.assertFalse(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), 11));
  }

  @Test
  public void testUnknownOperator() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("UNKNOWN_OP").setFieldType("STRING").setValue("test").build())
        .build();

    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, "test"));
  }

  @Test
  public void testUnsupportedFieldType() {
    BucketPredicate predicate = BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator("EQ").setFieldType("UNKNOWN_TYPE").setValue("test").build())
        .build();

    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, "test"));
  }

  @Test
  public void testPredicateTypeNotSet() {
    BucketPredicate predicate = BucketPredicate.newBuilder().build();
    Assert.assertFalse(PredicateEvaluator.evaluate(predicate, "test"));
  }

  @Test
  public void testNumberFormatException() {
    // Test invalid number format
    BucketPredicate intPredicate = createIntPredicate("GT", "not_a_number");
    Assert.assertFalse(PredicateEvaluator.evaluate(intPredicate, 10));

    BucketPredicate longPredicate = createLongPredicate("GT", "not_a_number");
    Assert.assertFalse(PredicateEvaluator.evaluate(longPredicate, 10L));

    BucketPredicate floatPredicate = createFloatPredicate("GT", "not_a_number");
    Assert.assertFalse(PredicateEvaluator.evaluate(floatPredicate, 10.5f));

    BucketPredicate doublePredicate = createDoublePredicate("GT", "not_a_number");
    Assert.assertFalse(PredicateEvaluator.evaluate(doublePredicate, 10.5));
  }

  @Test
  public void testTypeConversions() {
    // Test converting different types to expected types

    // Integer conversions
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), 10)); // Integer
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), 10L)); // Long to Int
    Assert.assertTrue(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), "10")); // String to Int
    Assert.assertFalse(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), "not_a_number")); // Invalid String

    // Long conversions
    Assert.assertTrue(PredicateEvaluator.evaluate(createLongPredicate("EQ", "10"), 10L)); // Long
    Assert.assertTrue(PredicateEvaluator.evaluate(createLongPredicate("EQ", "10"), 10)); // Integer to Long
    Assert.assertTrue(PredicateEvaluator.evaluate(createLongPredicate("EQ", "10"), "10")); // String to Long

    // Float conversions
    Assert.assertTrue(PredicateEvaluator.evaluate(createFloatPredicate("EQ", "10.5"), 10.5f)); // Float
    Assert.assertTrue(PredicateEvaluator.evaluate(createFloatPredicate("EQ", "10.5"), 10.5)); // Double to Float
    Assert.assertTrue(PredicateEvaluator.evaluate(createFloatPredicate("EQ", "10"), 10)); // Number to Float
    Assert.assertTrue(PredicateEvaluator.evaluate(createFloatPredicate("EQ", "10.5"), "10.5")); // String to Float

    // Double conversions
    Assert.assertTrue(PredicateEvaluator.evaluate(createDoublePredicate("EQ", "10.5"), 10.5)); // Double
    Assert.assertTrue(PredicateEvaluator.evaluate(createDoublePredicate("EQ", "10.5"), 10.5f)); // Float to Double
    Assert.assertTrue(PredicateEvaluator.evaluate(createDoublePredicate("EQ", "10"), 10)); // Number to Double
    Assert.assertTrue(PredicateEvaluator.evaluate(createDoublePredicate("EQ", "10.5"), "10.5")); // String to Double

    // Null conversions
    Assert.assertFalse(PredicateEvaluator.evaluate(createIntPredicate("EQ", "10"), null));
    Assert.assertFalse(PredicateEvaluator.evaluate(createLongPredicate("EQ", "10"), null));
    Assert.assertFalse(PredicateEvaluator.evaluate(createFloatPredicate("EQ", "10"), null));
    Assert.assertFalse(PredicateEvaluator.evaluate(createDoublePredicate("EQ", "10"), null));
    Assert.assertFalse(PredicateEvaluator.evaluate(createStringPredicate("EQ", "test"), null));
  }

  @Test
  public void testUtf8StringConversion() {
    // Test Utf8 to String conversion
    BucketPredicate stringPredicate = createStringPredicate("EQ", "test");
    Assert.assertTrue(PredicateEvaluator.evaluate(stringPredicate, new Utf8("test")));
    Assert.assertFalse(PredicateEvaluator.evaluate(stringPredicate, new Utf8("other")));
  }

  // Helper methods to create predicates
  private BucketPredicate createIntPredicate(String operator, String value) {
    return BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator(operator).setFieldType("INT").setValue(value).build())
        .build();
  }

  private BucketPredicate createLongPredicate(String operator, String value) {
    return BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator(operator).setFieldType("LONG").setValue(value).build())
        .build();
  }

  private BucketPredicate createFloatPredicate(String operator, String value) {
    return BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator(operator).setFieldType("FLOAT").setValue(value).build())
        .build();
  }

  private BucketPredicate createDoublePredicate(String operator, String value) {
    return BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator(operator).setFieldType("DOUBLE").setValue(value).build())
        .build();
  }

  private BucketPredicate createStringPredicate(String operator, String value) {
    return BucketPredicate.newBuilder()
        .setComparison(
            ComparisonPredicate.newBuilder().setOperator(operator).setFieldType("STRING").setValue(value).build())
        .build();
  }
}
