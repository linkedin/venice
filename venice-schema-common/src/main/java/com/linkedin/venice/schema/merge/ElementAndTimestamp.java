package com.linkedin.venice.schema.merge;

/**
 * A POJO containing an element in a collection (Map or List) and its corresponding timestamp which could be
 * either its active timestamp or its delete timestamp.
 */
class ElementAndTimestamp {
  private final Object element;
  private final long timestamp;

  ElementAndTimestamp(Object element, long timestamp) {
    this.element = element;
    this.timestamp = timestamp;
  }

  Object getElement() {
    return element;
  }

  long getTimestamp() {
    return timestamp;
  }
}
