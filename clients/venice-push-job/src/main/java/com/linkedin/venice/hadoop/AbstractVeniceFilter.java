package com.linkedin.venice.hadoop;

import java.io.Closeable;


/**
 * An abstraction to filter given data type. It can be used in conjunction with {@link FilterChain}.
 */
public abstract class AbstractVeniceFilter<INPUT_VALUE> implements Closeable {
  public AbstractVeniceFilter() {
  }

  /**
   * This function implements how to parse the value and determine if filtering is needed.
   * For certain value from Active/Active partial update enabled stores, it might filter out part of its input value and
   * only keep the remaining fresh part based on filter timestamp.
   * @return true if the value should be filtered out, otherwise false.
   */
  public abstract boolean checkAndMaybeFilterValue(final INPUT_VALUE value);
}
