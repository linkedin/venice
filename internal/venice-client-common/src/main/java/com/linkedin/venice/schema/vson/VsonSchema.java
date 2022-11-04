package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Deprecated
public final class VsonSchema implements Serializable {
  private static final long serialVersionUID = 1;

  private Object type;

  protected VsonSchema(Object type) {
    this.type = createValidType(type);
  }

  public static VsonSchema parse(String vsonSchemaStr) {
    return new VsonSchema(VsonSchemaAdapter.parse(vsonSchemaStr));
  }

  /**
   * Get the type created by selecting only a subset of properties from this
   * type. The type must be a map for this to work
   *
   * @param properties The properties to select
   * @return The new type definition
   */
  public VsonSchema projectionType(String... properties) {
    if (this.getType() instanceof Map<?, ?>) {
      Map<?, ?> type = (Map<?, ?>) getType();
      Arrays.sort(properties);
      Map<String, Object> newType = new LinkedHashMap<String, Object>();
      for (String prop: properties)
        newType.put(prop, type.get(prop));
      return new VsonSchema(newType);
    } else {
      throw new IllegalArgumentException("Cannot take the projection of a type that is not a Map.");
    }
  }

  public VsonSchema listSubtype() {
    if (type instanceof List) {
      return new VsonSchema(((List) type).get(0));
    } else {
      throw new IllegalArgumentException("listSubtype can be only used on List schema");
    }
  }

  public VsonSchema recordSubtype(String field) {
    if (this.getType() instanceof Map<?, ?>) {
      Map<?, ?> type = (Map<?, ?>) getType();
      return new VsonSchema(type.get(field));
    } else {
      throw new IllegalArgumentException("Cannot take the projection of a type that is not a Map.");
    }
  }

  public Object getType() {
    return this.type;
  }

  @Override
  public String toString() {
    return format(type);
  }

  public static String format(Object type) {
    StringBuilder b = new StringBuilder();
    if (type instanceof VsonTypes) {
      VsonTypes t = (VsonTypes) type;
      b.append('"');
      b.append(t.toDisplay());
      b.append('"');
    } else if (type instanceof List<?>) {
      b.append('[');
      List<?> l = (List<?>) type;
      for (Object o: l)
        b.append(format(o));
      b.append(']');
    } else if (type instanceof Map<?, ?>) {
      b.append('{');
      Map<?, ?> m = (Map<?, ?>) type;
      int i = 0;
      for (Map.Entry<?, ?> e: m.entrySet()) {
        b.append('"');
        b.append(e.getKey());
        b.append('"');
        b.append(':');
        b.append(format(e.getValue()));
        if (i < m.size() - 1)
          b.append(", ");
        i++;
      }
      b.append('}');
    } else {
      throw new VsonSerializationException(
          "Current type is " + type + " of class " + type.getClass() + " which is not allowed.");
    }

    return b.toString();
  }

  public void validate() {
    createValidType(getType());
  }

  private Object createValidType(Object type) {
    if (type == null) {
      throw new IllegalArgumentException("Type or subtype cannot be null.");
    } else if (type instanceof List<?>) {
      List<?> l = (List<?>) type;
      if (l.size() != 1)
        throw new IllegalArgumentException("Lists in type definition must have length exactly one.");
      return Arrays.asList(createValidType(l.get(0)));
    } else if (type instanceof Map<?, ?>) {
      @SuppressWarnings("unchecked")
      Map<String, ?> m = (Map<String, ?>) type;
      // bbansal: sort keys here for consistent with parse()
      Map<String, Object> newM = new LinkedHashMap<String, Object>(m.size());
      List<String> keys = new ArrayList<String>((m.keySet()));
      Collections.sort(keys);
      for (String key: keys)
        newM.put(key, createValidType(m.get(key)));
      return newM;
    } else if (type instanceof VsonTypes) {
      // this is good
      return type;
    } else {
      throw new IllegalArgumentException(
          "Unknown type in json type definition: " + type + " of class " + type.getClass().getName());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (o == null)
      return false;
    if (!o.getClass().equals(VsonSchema.class))
      return false;
    VsonSchema j = (VsonSchema) o;
    return getType().equals(j.getType());
  }

  @Override
  public int hashCode() {
    return getType().hashCode();
  }
}
