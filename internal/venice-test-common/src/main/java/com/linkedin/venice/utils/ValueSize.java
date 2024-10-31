package com.linkedin.venice.utils;

public enum ValueSize {
  SMALL_VALUE(false), LARGE_VALUE(true);

  public final boolean config;

  ValueSize(boolean config) {
    this.config = config;
  }
}
