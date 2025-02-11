package com.linkedin.venice.meta;

import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * This class is a handle to refer to a store-version.
 *
 * It intentionally does not contain any operational state related to the referenced store-version.
 *
 * It is appropriate to use as a map key. Its {@link #equals(Object)} and {@link #hashCode()} delegate to the same
 * functions on the String form of the store-version name.
 *
 * The purpose of using this handle class rather than a String is two-fold:
 *
 * - It is a stronger type than String, since a String can contain anything.
 * - It can be more performant, since shared instances are allocated once and reused thus causing less garbage, and
 *   shared instances are also faster to use as map keys (the hash code gets cached, and equality checks can be resolved
 *   by identity, in the common case).
 */
public class StoreVersionName {
  private final String name;
  private final StoreName storeName;
  private final int versionNumber;

  /**
   * Not intended to be called directly! Instead, use:
   *
   * {@link NameRepository#getStoreVersionName(String)}
   * {@link NameRepository#getStoreVersionName(String, int)}
   */
  StoreVersionName(String storeVersionName, StoreName storeName) {
    if (!Version.isVersionTopic(storeVersionName)) {
      throw new IllegalArgumentException(
          "Only valid store-version names are allowed! Valid names take the form of: storeName + \"_v\" + versionNumber");
    }
    this.name = Objects.requireNonNull(storeVersionName);
    this.storeName = Objects.requireNonNull(storeName);
    this.versionNumber = Version.parseVersionFromVersionTopicName(storeVersionName);
  }

  @Nonnull
  public String getName() {
    return this.name;
  }

  @Nonnull
  public StoreName getStore() {
    return this.storeName;
  }

  @Nonnull
  public String getStoreName() {
    return this.storeName.getName();
  }

  public int getVersionNumber() {
    return this.versionNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof StoreVersionName) {
      StoreVersionName that = (StoreVersionName) o;
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
