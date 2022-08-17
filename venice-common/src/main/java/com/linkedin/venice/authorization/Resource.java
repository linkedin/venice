package com.linkedin.venice.authorization;

/**
 * Represents a resource identified by "name".
 * This is an immutable class.
 */
public class Resource {
  private final String name;

  public Resource(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + name.hashCode();

    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Resource other = (Resource) o;
    if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Resource:" + name;
  }
}
