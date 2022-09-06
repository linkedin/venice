package com.linkedin.venice.schema.merge;

import org.apache.commons.lang3.Validate;


/**
 * A class contains a key and value entry for Avro map which only allows the key to be a String.
 */
class KeyValPair implements Comparable<KeyValPair> {
  private final String key;
  private final Object val;

  KeyValPair(String key) {
    this.key = Validate.notNull(key);
    this.val = null;
  }

  KeyValPair(String key, Object val) {
    this.key = Validate.notNull(key);
    this.val = val;
  }

  public String getKey() {
    return key;
  }

  public Object getVal() {
    return val;
  }

  @Override
  public int hashCode() {
    // It is intentional to use only the key's hash code.
    return key.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyValPair)) {
      return false;
    }
    KeyValPair other = (KeyValPair) o;
    return getKey().equals(other.getKey());
  }

  @Override
  public int compareTo(KeyValPair o) {
    return getKey().compareTo(o.getKey());
  }
}
