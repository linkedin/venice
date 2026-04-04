package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Represents the set of degraded datacenters for a Venice cluster.
 * Stored in a dedicated ZK node (separate from LiveClusterConfig) because
 * this is operational state, not cluster configuration.
 */
public class DegradedDcStates {
  @JsonProperty("degradedDatacenters")
  private Map<String, DegradedDcInfo> degradedDatacenters;

  public DegradedDcStates() {
    this.degradedDatacenters = new HashMap<>();
  }

  public DegradedDcStates(DegradedDcStates clone) {
    this.degradedDatacenters = new HashMap<>();
    if (clone.degradedDatacenters != null) {
      for (Map.Entry<String, DegradedDcInfo> entry: clone.degradedDatacenters.entrySet()) {
        this.degradedDatacenters.put(entry.getKey(), new DegradedDcInfo(entry.getValue()));
      }
    }
  }

  public Map<String, DegradedDcInfo> getDegradedDatacenters() {
    if (degradedDatacenters == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(degradedDatacenters);
  }

  public void setDegradedDatacenters(Map<String, DegradedDcInfo> degradedDatacenters) {
    this.degradedDatacenters = degradedDatacenters;
  }

  @JsonIgnore
  public Set<String> getDegradedDatacenterNames() {
    if (degradedDatacenters == null || degradedDatacenters.isEmpty()) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(degradedDatacenters.keySet());
  }

  @JsonIgnore
  public boolean isDatacenterDegraded(String dcName) {
    return degradedDatacenters != null && degradedDatacenters.containsKey(dcName);
  }

  @JsonIgnore
  public void addDegradedDatacenter(String dcName, DegradedDcInfo info) {
    if (degradedDatacenters == null) {
      degradedDatacenters = new HashMap<>();
    }
    degradedDatacenters.put(dcName, info);
  }

  @JsonIgnore
  public void removeDegradedDatacenter(String dcName) {
    if (degradedDatacenters != null) {
      degradedDatacenters.remove(dcName);
    }
  }

  @JsonIgnore
  public boolean isEmpty() {
    return degradedDatacenters == null || degradedDatacenters.isEmpty();
  }

  @Override
  public String toString() {
    return "DegradedDcStates{degradedDatacenters=" + degradedDatacenters + "}";
  }
}
