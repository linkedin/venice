package com.linkedin.venice.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Marks a test class or method as PubSub agnostic.
 * <p>
 * Tests with this annotation do not rely on Kafka-specific or Xinfra/NG-specific behavior.
 * They must pass against any PubSub system that implements the Venice PubSub APIs.
 * </p>
 *
 * <p><strong>Developer note:</strong>
 * If you annotate a test with this, you are asserting that it will run correctly on
 * any supported PubSub backend. You must ensure it stays this way, otherwise you risk
 * breaking CI pipelines that execute tests against multiple PubSub systems.
 * </p>
 *
 * Typical usage:
 * <pre>
 * {@code
 * @PubSubAgnosticTest
 * public class MyStoreTests { ... }
 *
 * public class AnotherTest {
 *   @Test
 *   @PubSubAgnosticTest
 *   public void validatesKeySerialization() { ... }
 * }
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface PubSubAgnosticTest {
  /**
   * Optional free-form notes for maintainers or the test runner.
   * Example: "Does not use client-specific seek APIs."
   */
  String[] notes() default {};
}
