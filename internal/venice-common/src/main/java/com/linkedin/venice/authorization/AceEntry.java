package com.linkedin.venice.authorization;

/**
 * Represents an Access Control Entry. {@link Permission} will be enforced when {@link Principal}
 * performs  {@link Method}. This is an immutable class.
 */
public class AceEntry {
  private final Principal principal;
  private final Method method;
  private final Permission permission;

  /**
   * Creates an Access Control entry
   *
   * @param principal {@link Principal} the actor.
   * @param method {@link Method} the method.
   * @param permission {@link Permission} the permission.
   */
  public AceEntry(Principal principal, Method method, Permission permission) {
    this.principal = principal;
    this.method = method;
    this.permission = permission;
  }

  public Principal getPrincipal() {
    return principal;
  }

  public Method getMethod() {
    return method;
  }

  public Permission getPermission() {
    return permission;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + principal.hashCode();
    result = result * 31 + method.hashCode();
    result = result * 31 + permission.hashCode();

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

    AceEntry other = (AceEntry) o;
    if (!principal.equals(other.principal)) {
      return false;
    }
    if (!method.equals(other.method)) {
      return false;
    }
    if (!permission.equals(other.permission)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{")
        .append(principal)
        .append(", Method:")
        .append(method)
        .append(", Permission:")
        .append(permission)
        .append("}");
    return sb.toString();
  }
}
