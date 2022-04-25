package com.linkedin.venice.authorization;

/**
 * Represents an actor entry. For implementation purpose the name may be formatted as needed.
 * This is an immutable class.
 */
public class Principal {
  private final String name;

  public Principal(String name) {
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

    Principal other = (Principal) o;
    if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Principal:" + name;
  }
}
