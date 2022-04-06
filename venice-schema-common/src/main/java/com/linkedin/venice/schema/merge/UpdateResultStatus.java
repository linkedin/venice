package com.linkedin.venice.schema.merge;

/**
 * This enum represents status of something (a record or a field) after being updated. Currently, both field and record
 * can share these 3 update result statuses.
 */
public enum UpdateResultStatus {
  NOT_UPDATED_AT_ALL, // Not updated nor changed at all.
  PARTIALLY_UPDATED, // Partially updated.
  COMPLETELY_UPDATED // Completely updated. For example, a field/record could be deleted or replaced completely.
}
