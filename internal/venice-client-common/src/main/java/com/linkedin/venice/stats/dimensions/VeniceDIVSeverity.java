package com.linkedin.venice.stats.dimensions;

/**
 * Dimension enum representing the severity of a leader offset rewind event.
 * Maps to {@link VeniceMetricsDimensions#VENICE_DIV_SEVERITY}.
 */
public enum VeniceDIVSeverity implements VeniceDimensionInterface {
  /** Offset rewind where data in the rewound range is still present — no data loss. */
  BENIGN,
  /** Offset rewind where data in the rewound range may no longer be present — possible data loss. */
  POTENTIALLY_LOSSY;

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_DIV_SEVERITY;
  }
}
