package com.linkedin.venice.authorization;

/**
 * A list of permission that will determine what {@link Method} a {@link Principal} can perform on a {@link Resource}.
 */
public enum Permission {
  ALLOW, DENY, UNKNOWN
}
