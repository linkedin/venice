package com.linkedin.venice.utils.lazy;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ResolvedLazyTest {
  @Test
  public void testOfValueWithNonNull() {
    Lazy<String> lazy = Lazy.ofValue("hello");

    Assert.assertTrue(lazy.isPresent());
    Assert.assertEquals(lazy.get(), "hello");

    AtomicBoolean called = new AtomicBoolean(false);
    lazy.ifPresent(v -> called.set(true));
    Assert.assertTrue(called.get());

    Optional<Integer> mapped = lazy.map(String::length);
    Assert.assertTrue(mapped.isPresent());
    Assert.assertEquals(mapped.get().intValue(), 5);

    Optional<Integer> flatMapped = lazy.flatMap(v -> Optional.of(v.length()));
    Assert.assertTrue(flatMapped.isPresent());
    Assert.assertEquals(flatMapped.get().intValue(), 5);

    Optional<String> filtered = lazy.filter(v -> v.startsWith("h"));
    Assert.assertTrue(filtered.isPresent());
    Assert.assertEquals(filtered.get(), "hello");

    Optional<String> filteredOut = lazy.filter(v -> v.startsWith("x"));
    Assert.assertFalse(filteredOut.isPresent());

    Assert.assertEquals(lazy.orElse("other"), "hello");
    Assert.assertEquals(lazy.orElseGet(() -> "other"), "hello");
    Assert.assertEquals(lazy.orElseThrow(IllegalStateException::new), "hello");
  }

  @Test
  public void testOfValueWithNull() {
    Lazy<String> lazy = Lazy.ofValue(null);

    Assert.assertTrue(lazy.isPresent());
    Assert.assertNull(lazy.get());
    Assert.assertNull(lazy.orElse("other"));
    Assert.assertNull(lazy.orElseGet(() -> "other"));
  }

  @Test
  public void testStaticConstants() {
    Assert.assertFalse(Lazy.FALSE.get());
    Assert.assertTrue(Lazy.TRUE.get());
    Assert.assertTrue(Lazy.FALSE.isPresent());
    Assert.assertTrue(Lazy.TRUE.isPresent());
  }
}
