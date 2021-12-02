package com.linkedin.venice.schema.rmd;

public class ReplicationMetadataVersionId {
  int valueSchemaVersion;
  int replicationMetadataProtocolVersion;

  public ReplicationMetadataVersionId(int valueSchemaVersion, int replicationMetadataProtocolVersion) {
    this.valueSchemaVersion = valueSchemaVersion;
    this.replicationMetadataProtocolVersion = replicationMetadataProtocolVersion;
  }

  public int getValueSchemaVersion() {
    return valueSchemaVersion;
  }

  public int getReplicationMetadataProtocolVersion() {
    return replicationMetadataProtocolVersion;
  }

  @Override
  public final int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + valueSchemaVersion;
    result = PRIME * result + replicationMetadataProtocolVersion;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ReplicationMetadataVersionId that = (ReplicationMetadataVersionId)o;

    if (valueSchemaVersion != that.valueSchemaVersion) return false;
    if (replicationMetadataProtocolVersion != that.replicationMetadataProtocolVersion) return false;

    return true;
  }
}
