package com.linkedin.venice.schema.rmd;

public class RmdVersionId {
  private final int valueSchemaID;
  private final int replicationMetadataProtocolVersion;

  public RmdVersionId(int valueSchemaID, int replicationMetadataProtocolVersion) {
    this.valueSchemaID = valueSchemaID;
    this.replicationMetadataProtocolVersion = replicationMetadataProtocolVersion;
  }

  public int getValueSchemaID() {
    return valueSchemaID;
  }

  public int getReplicationMetadataProtocolVersion() {
    return replicationMetadataProtocolVersion;
  }

  @Override
  public final int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + valueSchemaID;
    result = PRIME * result + replicationMetadataProtocolVersion;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    RmdVersionId that = (RmdVersionId) o;

    if (valueSchemaID != that.valueSchemaID)
      return false;
    if (replicationMetadataProtocolVersion != that.replicationMetadataProtocolVersion)
      return false;

    return true;
  }
}
