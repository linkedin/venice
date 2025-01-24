package com.linkedin.venice.meta;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;


/**
 * This class is a handle to refer to a store.
 *
 * It intentionally does not contain any operational state related to the referenced store.
 *
 * It is appropriate to use as a map key. Its {@link #equals(Object)} and {@link #hashCode()} delegate to the same
 * functions on the String form of the store name.
 *
 * The purpose of using this handle class rather than a String is two-fold:
 *
 * - It is a stronger type than String, since a String can contain anything.
 * - It can be more performant, since shared instances are allocated once and reused thus causing less garbage, and
 *   shared instances are also faster to use as map keys (the hash code gets cached, and equality checks can be resolved
 *   by identity, in the common case).
 */
public class StoreName {
  private static final Pattern STORE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]+$");

  private final String name;

  /**
   * Not intended to be called directly! Instead, use:
   *
   * {@link NameRepository#getStoreName(String)}
   */
  StoreName(String name) {
    if (!isValidStoreName(name)) {
      throw new IllegalArgumentException("Invalid store name!");
    }
    this.name = Objects.requireNonNull(name);
  }

  /**
   * Store name rules:
   *  1. Only letters, numbers, underscore or dash
   *  2. No double dashes
   */
  public static boolean isValidStoreName(String name) {
    Matcher matcher = STORE_NAME_PATTERN.matcher(name);
    return matcher.matches() && !name.contains("--");
  }

  @Nonnull
  public String getName() {
    return this.name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof StoreName) {
      StoreName that = (StoreName) o;
      return this.name.equals(that.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + this.name + ")";
  }
}
