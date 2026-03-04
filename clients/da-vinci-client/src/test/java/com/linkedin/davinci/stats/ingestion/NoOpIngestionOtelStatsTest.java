package com.linkedin.davinci.stats.ingestion;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public class NoOpIngestionOtelStatsTest {
  /**
   * Ensures every public instance method declared in {@link IngestionOtelStats} is overridden
   * in {@link NoOpIngestionOtelStats}. If someone adds a new recording method to
   * {@code IngestionOtelStats} without adding a no-op override, this test will fail — preventing
   * accidental NPEs or real metric registrations when ingestion OTel stats are disabled.
   */
  @Test
  public void testAllPublicMethodsAreOverridden() {
    List<String> missingOverrides = new ArrayList<>();

    for (Method method: IngestionOtelStats.class.getDeclaredMethods()) {
      if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      try {
        Method override = NoOpIngestionOtelStats.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
        if (override.getDeclaringClass() != NoOpIngestionOtelStats.class) {
          missingOverrides.add(method.getName());
        }
      } catch (NoSuchMethodException e) {
        missingOverrides.add(method.getName());
      }
    }

    assertEquals(
        missingOverrides,
        Collections.emptyList(),
        "NoOpIngestionOtelStats must override all public methods from IngestionOtelStats. Missing: "
            + missingOverrides);
  }
}
