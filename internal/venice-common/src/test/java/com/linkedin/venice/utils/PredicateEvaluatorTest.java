package com.linkedin.venice.utils;

import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.ComparisonPredicate;
import com.linkedin.venice.protocols.LogicalPredicate;
import java.util.Arrays;
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
}
