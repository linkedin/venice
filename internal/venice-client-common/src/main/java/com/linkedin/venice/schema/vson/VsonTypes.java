package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;


@Deprecated
public enum VsonTypes {
  BOOLEAN("boolean"), STRING("string"), INT8("int8"), INT16("int16"), INT32("int32"), INT64("int64"),
  FLOAT32("float32"), FLOAT64("float64"), BYTES("bytes"), DATE("date");

  private final String display;

  VsonTypes(String display) {
    this.display = display;
  }

  public String toDisplay() {
    return display;
  }

  public static VsonTypes fromDisplay(String name) {
    for (VsonTypes t: VsonTypes.values())
      if (t.toDisplay().equals(name))
        return t;
    throw new VsonSerializationException(name + " is not a valid display for any SimpleType.");
  }
}
