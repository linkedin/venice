package com.linkedin.venice.testng;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;


/**
 * TestNG annotation transformer that sets {@link VeniceTestRetryAnalyzer} as the default
 * retry analyzer for all {@code @Test} methods that don't already have one.
 *
 * <p>This ensures per-invocation retry granularity: when a data-provider-parameterized test fails
 * for one parameter combination, only that combination is retried — not all combinations.
 *
 * <p>Register in build.gradle via:
 * <pre>
 *   useTestNG {
 *     listeners = ['com.linkedin.venice.testng.VeniceTestRetryAnnotationTransformer', ...]
 *   }
 * </pre>
 *
 * <p>When this transformer sets a retry analyzer, the Gradle test-retry plugin automatically
 * defers to TestNG's native retry for that method (no double-retry).
 */
public class VeniceTestRetryAnnotationTransformer implements IAnnotationTransformer {
  @Override
  public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
    if (annotation.getRetryAnalyzer() == null) {
      annotation.setRetryAnalyzer(VeniceTestRetryAnalyzer.class);
    }
  }
}
