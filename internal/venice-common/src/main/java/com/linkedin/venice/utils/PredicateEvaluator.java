package com.linkedin.venice.utils;

import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.ComparisonPredicate;
import com.linkedin.venice.protocols.LogicalPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility class for evaluating BucketPredicate from protobuf definitions.
 * Supports comparison predicates (GT, GTE, LT, LTE, EQ, IN) and logical predicates (AND, OR).
 */
public class PredicateEvaluator {
  private static final Logger LOGGER = LogManager.getLogger(PredicateEvaluator.class);

  private PredicateEvaluator() {
    // Utility class
  }

  /**
   * Evaluate a BucketPredicate against a field value.
   * 
   * @param predicate The predicate to evaluate
   * @param fieldValue The field value to test
   * @return true if the predicate matches, false otherwise
   */
  public static boolean evaluate(BucketPredicate predicate, Object fieldValue) {
    if (predicate == null || fieldValue == null) {
      return false;
    }

    switch (predicate.getPredicateTypeCase()) {
      case COMPARISON:
        return evaluateComparison(predicate.getComparison(), fieldValue);
      case LOGICAL:
        return evaluateLogical(predicate.getLogical(), fieldValue);
      case PREDICATETYPE_NOT_SET:
        LOGGER.warn("Predicate type not set");
        return false;
      default:
        LOGGER.warn("Unknown predicate type: {}", predicate.getPredicateTypeCase());
        return false;
    }
  }

  /**
   * Evaluate a comparison predicate.
   */
  private static boolean evaluateComparison(ComparisonPredicate comparison, Object fieldValue) {
    String operator = comparison.getOperator();
    String fieldType = comparison.getFieldType();
    String predicateValue = comparison.getValue();

    try {
      switch (fieldType.toUpperCase()) {
        case "LONG":
          return evaluateLongComparison(operator, toLong(fieldValue), Long.parseLong(predicateValue));
        case "INT":
          return evaluateIntComparison(operator, toInt(fieldValue), Integer.parseInt(predicateValue));
        case "FLOAT":
          return evaluateFloatComparison(operator, toFloat(fieldValue), predicateValue);
        case "DOUBLE":
          return evaluateDoubleComparison(operator, toDouble(fieldValue), predicateValue);
        case "STRING":
          return evaluateStringComparison(operator, toString(fieldValue), predicateValue);
        default:
          LOGGER.warn("Unsupported field type: {}", fieldType);
          return false;
      }
    } catch (NumberFormatException e) {
      LOGGER.warn("Failed to parse predicate value '{}' as {}: {}", predicateValue, fieldType, e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.warn("Failed to evaluate comparison: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Evaluate a logical predicate (AND/OR).
   */
  private static boolean evaluateLogical(LogicalPredicate logical, Object fieldValue) {
    String operator = logical.getOperator();

    switch (operator.toUpperCase()) {
      case "AND":
        // All predicates must be true
        for (BucketPredicate pred: logical.getPredicatesList()) {
          if (!evaluate(pred, fieldValue)) {
            return false;
          }
        }
        return true;
      case "OR":
        // At least one predicate must be true
        for (BucketPredicate pred: logical.getPredicatesList()) {
          if (evaluate(pred, fieldValue)) {
            return true;
          }
        }
        return false;
      default:
        LOGGER.warn("Unknown logical operator: {}", operator);
        return false;
    }
  }

  /**
   * Evaluate comparison for Long values.
   */
  private static boolean evaluateLongComparison(String operator, Long fieldValue, Long predicateValue) {
    if (fieldValue == null) {
      return false;
    }

    switch (operator.toUpperCase()) {
      case "GT":
        return fieldValue > predicateValue;
      case "GTE":
        return fieldValue >= predicateValue;
      case "LT":
        return fieldValue < predicateValue;
      case "LTE":
        return fieldValue <= predicateValue;
      case "EQ":
        return fieldValue.equals(predicateValue);
      case "IN":
        // For single value IN, just check equality
        return fieldValue.equals(predicateValue);
      default:
        LOGGER.warn("Unknown operator: {}", operator);
        return false;
    }
  }

  /**
   * Evaluate comparison for Integer values.
   */
  private static boolean evaluateIntComparison(String operator, Integer fieldValue, Integer predicateValue) {
    if (fieldValue == null) {
      return false;
    }

    switch (operator.toUpperCase()) {
      case "GT":
        return fieldValue > predicateValue;
      case "GTE":
        return fieldValue >= predicateValue;
      case "LT":
        return fieldValue < predicateValue;
      case "LTE":
        return fieldValue <= predicateValue;
      case "EQ":
        return fieldValue.equals(predicateValue);
      case "IN":
        return fieldValue.equals(predicateValue);
      default:
        LOGGER.warn("Unknown operator: {}", operator);
        return false;
    }
  }

  /**
   * Evaluate comparison for Float values.
   */
  private static boolean evaluateFloatComparison(String operator, Float fieldValue, String predicateValueStr) {
    if (fieldValue == null) {
      return false;
    }

    switch (operator.toUpperCase()) {
      case "GT":
        return fieldValue > Float.parseFloat(predicateValueStr);
      case "GTE":
        return fieldValue >= Float.parseFloat(predicateValueStr);
      case "LT":
        return fieldValue < Float.parseFloat(predicateValueStr);
      case "LTE":
        return fieldValue <= Float.parseFloat(predicateValueStr);
      case "EQ":
        return Math.abs(fieldValue - Float.parseFloat(predicateValueStr)) < 0.0001f; // Float comparison with epsilon
      case "IN":
        // For IN operator with floats, support comma-separated values
        String[] values = predicateValueStr.split(",");
        for (String value: values) {
          try {
            float val = Float.parseFloat(value.trim());
            if (Math.abs(fieldValue - val) < 0.0001f) {
              return true;
            }
          } catch (NumberFormatException e) {
            continue;
          }
        }
        return false;
      default:
        LOGGER.warn("Unknown operator: {}", operator);
        return false;
    }
  }

  /**
   * Evaluate comparison for Double values.
   */
  private static boolean evaluateDoubleComparison(String operator, Double fieldValue, String predicateValueStr) {
    if (fieldValue == null) {
      return false;
    }

    switch (operator.toUpperCase()) {
      case "GT":
        return fieldValue > Double.parseDouble(predicateValueStr);
      case "GTE":
        return fieldValue >= Double.parseDouble(predicateValueStr);
      case "LT":
        return fieldValue < Double.parseDouble(predicateValueStr);
      case "LTE":
        return fieldValue <= Double.parseDouble(predicateValueStr);
      case "EQ":
        return Math.abs(fieldValue - Double.parseDouble(predicateValueStr)) < 0.0000001; // Double comparison with
                                                                                         // epsilon
      case "IN":
        // For IN operator with doubles, support comma-separated values
        String[] values = predicateValueStr.split(",");
        for (String value: values) {
          try {
            double val = Double.parseDouble(value.trim());
            if (Math.abs(fieldValue - val) < 0.0000001) {
              return true;
            }
          } catch (NumberFormatException e) {
            continue;
          }
        }
        return false;
      default:
        LOGGER.warn("Unknown operator: {}", operator);
        return false;
    }
  }

  /**
   * Evaluate comparison for String values.
   */
  private static boolean evaluateStringComparison(String operator, String fieldValue, String predicateValue) {
    if (fieldValue == null) {
      return false;
    }

    switch (operator.toUpperCase()) {
      case "GT":
        return fieldValue.compareTo(predicateValue) > 0;
      case "GTE":
        return fieldValue.compareTo(predicateValue) >= 0;
      case "LT":
        return fieldValue.compareTo(predicateValue) < 0;
      case "LTE":
        return fieldValue.compareTo(predicateValue) <= 0;
      case "EQ":
        return fieldValue.equals(predicateValue);
      case "IN":
        // For IN operator with strings, support comma-separated values
        String[] values = predicateValue.split(",");
        for (String value: values) {
          if (fieldValue.equals(value.trim())) {
            return true;
          }
        }
        return false;
      default:
        LOGGER.warn("Unknown operator: {}", operator);
        return false;
    }
  }

  /**
   * Convert field value to Long.
   */
  private static Long toLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Integer) {
      return ((Integer) value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert field value to Integer.
   */
  private static Integer toInt(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value instanceof Long) {
      return ((Long) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert field value to Float.
   */
  private static Float toFloat(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Float) {
      return (Float) value;
    }
    if (value instanceof Double) {
      return ((Double) value).floatValue();
    }
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }
    if (value instanceof String) {
      try {
        return Float.parseFloat((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert field value to Double.
   */
  private static Double toDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Double) {
      return (Double) value;
    }
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof String) {
      try {
        return Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Convert field value to String.
   */
  private static String toString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof org.apache.avro.util.Utf8) {
      return value.toString();
    }
    return value.toString();
  }
}
