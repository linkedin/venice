package com.linkedin.venice.schema;

public class MetadataVersionId {
  int valueSchemaVersion;
  int metadataProtocolVersion;

  public MetadataVersionId(int valueSchemaVersion, int metadataProtocolVersion) {
    this.valueSchemaVersion = valueSchemaVersion;
    this.metadataProtocolVersion = metadataProtocolVersion;
  }

  public int getValueSchemaVersion() {
    return valueSchemaVersion;
  }

  public int getMetadataProtocolVersion() {
    return metadataProtocolVersion;
  }

  @Override
  public final int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + valueSchemaVersion;
    result = PRIME * result + metadataProtocolVersion;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MetadataVersionId that = (MetadataVersionId)o;

    if (valueSchemaVersion != that.valueSchemaVersion) return false;
    if (metadataProtocolVersion != that.metadataProtocolVersion) return false;

    return true;
  }
}
