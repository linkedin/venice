package com.linkedin.venice.schema.rmd;

public class ReplicationMetadataVersionId {
  private final int valueSchemaID;
  private final int replicationMetadataProtocolVersion;

  public ReplicationMetadataVersionId(int valueSchemaID, int replicationMetadataProtocolVersion) {
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ReplicationMetadataVersionId that = (ReplicationMetadataVersionId)o;

    if (valueSchemaID != that.valueSchemaID) return false;
    if (replicationMetadataProtocolVersion != that.replicationMetadataProtocolVersion) return false;

    return true;
  }
}
