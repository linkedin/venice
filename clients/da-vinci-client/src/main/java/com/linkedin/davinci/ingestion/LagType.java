package com.linkedin.davinci.ingestion;

public enum LagType {
  OFFSET_LAG("Offset"), TIME_LAG("Time");

  private final String prettyString;

  LagType(String prettyString) {
    this.prettyString = prettyString;
  }

  public String prettyString() {
    return this.prettyString;
  }
}
