package com.linkedin.venice.schema;

public class TimestampMetadataVersionId {
  int valueSchemaVersion;
  int timestampMetadataProtocolVersion;

  public TimestampMetadataVersionId(int valueSchemaVersion, int timestampMetadataProtocolVersion) {
    this.valueSchemaVersion = valueSchemaVersion;
    this.timestampMetadataProtocolVersion = timestampMetadataProtocolVersion;
  }

  public int getValueSchemaVersion() {
    return valueSchemaVersion;
  }

  public int getTimestampMetadataProtocolVersion() {
    return timestampMetadataProtocolVersion;
  }

  @Override
  public final int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + valueSchemaVersion;
    result = PRIME * result + timestampMetadataProtocolVersion;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TimestampMetadataVersionId that = (TimestampMetadataVersionId)o;

    if (valueSchemaVersion != that.valueSchemaVersion) return false;
    if (timestampMetadataProtocolVersion != that.timestampMetadataProtocolVersion) return false;

    return true;
  }
}
